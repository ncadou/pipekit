#!/usr/bin/env python3

import asyncio
from functools import partial
from itertools import chain
from threading import Barrier, Thread

import janus
from box import Box

from . import pipe
from .component import Component, ComponentInterrupted
from .message import Message
from .utils import aiter, isdict, islist


class Node(Component):
    """Processes messages."""

    def configure(self, process=None, inbox=None, ifilters=None, ofilters=None, outbox=None,
                  conditions=None, blocking=False, scale=None, **settings):
        if callable(process):
            self.process = process
        self.inbox = self._join_pipes(inbox, Inbox)
        self.ifilters = ifilters or PriorityRegistry()
        self.ofilters = ofilters or PriorityRegistry()
        for filter_ in chain(self.ifilters.values(), self.ofilters.values()):
            filter_.parent = self
        self.outbox = self._join_pipes(outbox, Outbox)
        self.children = [self.inbox, self.outbox]
        self.conditions = conditions or []
        self.blocking = self.__class__ is ThreadedNode or blocking
        self.scale = int(scale or 1)
        self.layers = list()
        self.locals = Box()

        # Used by ProcessorWrapper to control behavior of input feed.
        self.wait_for_pending = True

        return settings

    def __str__(self):
        process_str = ''
        if not hasattr(self.process, 'stub') and not hasattr(self.process, 'overloaded'):
            process_str = f' {self.process}'
        return f'<{self.type} {self.id}{process_str}>'

    def _join_pipes(self, pipes, class_):
        if not isinstance(pipes, dict):
            pipes = dict(default=pipes)
        msgbox = self.workflow.make_component(class_, id=f'{self.id}.{class_.__name__.lower()}',
                                              parent=self, **pipes)
        return msgbox

    _dependent_statuses = set()

    def start(self, *args):
        coroutines = [super().start(*args)]
        self.layers = ([self.inbox] +
                       self.ifilters.ordered() +
                       self.spawn_processor() +
                       self.ofilters.ordered() +
                       [self.outbox])
        for layer in self.layers:
            if isinstance(layer, Component):
                coroutines.append(layer.start())
        return asyncio.gather(*coroutines)

    async def run(self):
        if not self.aborted:
            await super().run()
            conditions = set(self.conditions)
            while self.running and conditions:
                for event in conditions.copy():
                    if self.hasstatus(f'{event.rsplit(":", 1)[0]}:aborted'):
                        self.abort()
                    elif self.hasstatus(event):
                        conditions.remove(event)
                if conditions:
                    await asyncio.sleep(0.1)

        self.status = 'cleared'
        if self.running:
            try:
                await self._run()
            except Exception as exc:
                self.exception('Fatal error')
                self.abort(exc)
                if False and True:  # TODO: make this configurable
                    raise

        if not self.aborted:
            self.status = 'finished'
        self.status = 'exited'

    async def _run(self):
        if not self.running:
            return

        stack = self.layers[0]
        for layer in self.layers[1:]:
            stack = layer(stack)
        async for result in stack:
            if not self.running:
                break

    def spawn_processor(self):
        self._processor = ProcessorWrapper(self)
        return [self._pre_processor, self._processor, self._post_processor]

    async def _pre_processor(self, messages):
        """Generate an event on the first message reaching the processor."""
        first_message = True
        async for channel, message in messages:
            if not self.running:
                break

            if first_message:
                self.status = 'processing-started'
                first_message = False
            yield (channel, message)

    async def _post_processor(self, messages):
        """Generate an event on the last message leaving the processor."""
        async for channel, message in messages:
            if not self.running:
                break

            yield (channel, message)
        self.status = 'processing-finished'

    async def processor(self, messages, id_=None):
        """Process and yield new (channel, message)."""
        async for channel, message in messages:
            yield await self.process(channel, message)

    async def process(self, channel, message):
        raise NotImplementedError(f'process() in {self}')

    process.stub = True

    def drop(self, message):  # TODO: implement message accounting and leak detection
        self._processor.drop(message)

    async def retry(self, channel, message, wait=False):
        """Insert a message back into the input queue for retrying it."""
        await self._processor.retry(channel, message, wait)

    def merged_settings(self, message, key=None, msgmap=None):
        """Return node settings, with values overridden from the message if present."""
        settings = self.settings.to_dict()
        message = message.to_dict()
        if key:
            settings = settings[key]
            message = message[key]
        if msgmap is None:
            msgmap = list(settings)
        if islist(msgmap):
            msgmap = dict(zip(msgmap, msgmap))
        elif not isdict(msgmap):
            raise TypeError(f'Argument msgmap must be a mapping or a sequence, not {type(msgmap)}')

        return Box(settings, **dict((arg, message[msgarg]) for arg, msgarg in msgmap.items()))

    __MISSING__ = object()

    def named_value(self, key, message=None, default=__MISSING__, settings=None):
        """Return `message`'s attribute named by settings' `key` element.

        If `default` is provided, the element will be given this value if
        it doesn't exist yet.

        We use the `__MISSING__` value because `None` is a valid default.

        """
        if settings is None:
            settings = self._settings
        attrs = settings.setdefault('attrs', {})
        if default is not self.__MISSING__:
            attrs.setdefault(key, default)
        if hasattr(message, 'data'):
            return message.data[attrs[key]]


class WithRetry:
    pass


class ProcessorWrapper:
    """Wraps `Node.processor()` to provide retry mechanism and parallelism."""

    def __init__(self, node):
        self.node = node
        self.running = True
        self.exc = None

    async def infeed(self, messages):
        """Loop through all messages from the inbox and relay them to the input queue."""
        async for channel, message in messages:
            self.pending[f'{id(message)}-{message.meta.id}'] = message
            await self.iqueue.put((channel, message))

        if self.pending:
            if self.node.wait_for_pending:
                pending = None
                while self.pending and self.node.running:
                    if pending != len(self.pending):
                        pending = len(self.pending)
                    await asyncio.sleep(0.1)
            if self.pending:
                plural = 's' if len(self.pending) != 1 else ''
                self.node.warning(f'ProcessorWrapper exiting with {len(self.pending)} pending '
                                  f'message{plural}')
        self.running = False

    async def _input_iter(self):
        wait = self.iwait
        while self.node.running and self.running:
            queues_are_empty = True
            for queue in (self.rqueue, self.iqueue):
                try:
                    yield queue.get_nowait()

                    queues_are_empty = False
                except asyncio.QueueEmpty:
                    pass

            if queues_are_empty:
                await asyncio.sleep(wait)
                if wait < 0.1:
                    wait *= 2
            else:
                wait = self.iwait

    async def collector(self, id_):
        """Launch an instance of the node processor, and relay its output to the output queue."""
        async for channel, message in self.node.processor(self._input_iter(), id_=id_):
            await self.oqueue.put((channel, message))
        await self.oqueue.put((None, pipe.EOT))

    async def outfeed(self, messages):
        """Launch all processor instances and return an iterator that feeds on the output queue."""
        self.iqueue = asyncio.Queue(maxsize=self.node.scale)  # input queue
        self.rqueue = asyncio.Queue()                         # retry queue
        self.oqueue = asyncio.Queue(maxsize=1)                # output queue
        self.iwait = 0.001  # initial scaleback wait value for input queue
        self.pending = dict()  # messages currently handled by node processor
        coroutines = list()
        for i in range(self.node.scale):
            coroutines.append(asyncio.ensure_future(self.collector(i)))
        coroutines.append(asyncio.ensure_future(self.infeed(messages)))
        running = self.node.scale
        wait = self.iwait

        while running or not self.oqueue.empty():
            if self.oqueue.empty():
                # Raise any exception from collectors.
                for coroutine in coroutines:
                    try:
                        # self.node.debug('*** error?')
                        coroutine.result()
                    except asyncio.InvalidStateError:
                        # self.node.debug('*** nope')
                        pass

                if self.node.hasstatus('aborted'):
                    break

                await asyncio.sleep(wait)
                if wait < 0.1:
                    wait *= 2
                continue

            else:
                wait = self.iwait

            channel, message = await self.oqueue.get()
            self.oqueue.task_done()
            if message is pipe.EOT:
                running -= 1
            else:
                self.pending.pop(f'{id(message)}-{message.meta.id}', None)
                yield channel, message

        self.running = False
        for coroutine in coroutines:
            if self.node.hasstatus('aborted'):
                coroutine.cancel()
            try:
                await coroutine
            except asyncio.CancelledError:
                pass

    def drop(self, message, component=None):
        """Drop message and stop tracking it."""
        self.pending.pop(f'{id(message)}-{message.meta.id}', None)
        message.drop(component or self.node)

    async def retry(self, channel, message, wait=False):
        """Insert a message back into the input queue for retrying it."""
        self.node.debug(f'Retrying message {message}')
        self.pending[f'{id(message)}-{message.meta.id}'] = message

        async def retry(channel, message, wait):
            if wait:
                await asyncio.sleep(wait)  # tight loop mitigation
            await self.rqueue.put((channel, message))

        if wait is None:
            wait = 0.000001

        # Schedule push to retry queue to free up caller.
        self.node.debug(f'Requeuing message {message}')
        asyncio.ensure_future(retry(channel, message, wait))

    __call__ = outfeed


class Inbox(pipe.Manifold):

    async def receiver(self):
        message = None
        active_channels = 0
        last_channel = False
        self.debug('Ready to receive')
        for channel, _pipe in self._active_channels():
            if not self.running:
                return

            if _pipe is None:
                if active_channels == 1:
                    last_channel = True
                if message is False:  # all channels are empty
                    await asyncio.sleep(0.05)

                message = False
                active_channels = 0
                continue

            active_channels += 1
            try:
                message = await self.try_while_running(
                        partial(_pipe.receive, wait=last_channel))
            except asyncio.QueueEmpty:
                continue

            except ComponentInterrupted:
                return

            except Exception:
                self.exception(f'Error while receiving on channel {channel}, {_pipe}')
                raise

            self.debug(f'Got message from {channel}: {message}')
            if message == pipe.EOT:
                _pipe.stop()
            else:
                message.checkin(self)
                yield (channel, message)

        self.debug('Finished receiving')

    def _active_channels(self):
        active_channels = self.channels.copy()
        while active_channels:
            yield None, None  # signal start of channels sweep

            for channel, _pipe in active_channels.copy().items():
                if not _pipe.running:
                    self.debug(f'Skipping stopped channel {channel}')
                    del active_channels[channel]
                else:
                    yield channel, _pipe

    async def channel_receiver(self, channel, feeder, events):
        self.debug(f'Receiver for {channel} is ready')
        pipe = self.channels[channel]
        while pipe.running and self.running and await events[channel].wait():
            events[channel].clear()
            try:
                feeder.append((channel, await pipe.receive()))
            except Exception:
                self.exception(f'Error while receiving on channel {channel}, {pipe}')
                raise

            # self.debug(f'* received from channel {channel} (next event: {events[channel].is_set()})')  # noqa: E501
            events.received.set()
        self.debug(f'Receiver for {channel} is exiting')


class Outbox(pipe.Manifold):
    def __call__(self, messages):
        return self._sender(messages)

    async def _sender(self, messages):
        self.debug('Ready to send')
        try:
            async for channel, message in self.sender(messages):
                yield channel, message

        except Exception:
            self.status = 'aborted'
            raise

        else:
            self.status = 'finished'
        finally:
            self.status = 'exited'

    async def sender(self, messages):
        async for channel, message in messages:
            channel = channel or 'default'
            if not self.running:
                break

            if channel is Message.DROP:
                self.parent.drop(message)
            else:
                self.debug(f'Sending message to {channel}: {message}')
                if channel in self.channels:
                    try:
                        await self.try_while_running(partial(self.channels[channel].send, message))
                    except ComponentInterrupted:
                        break

                    message.checkout(self)
                    yield channel, message

                else:
                    raise KeyError(f'Channel "{channel}" does not exist in {self}')

        self.debug('Finished sending')
        try:
            for _pipe in self.channels.values():
                await self.try_while_running(partial(_pipe.send, pipe.EOT))
        except ComponentInterrupted:
            pass


class ThreadedNode(Node):
    pass


class ThreadedProcessor:
    def __init__(self, processor, scale):
        self.processor = processor
        self.scale = scale
        self.iqueue = janus.Queue(maxsize=self.scale)
        self.oqueue = janus.Queue(maxsize=self.scale)
        self.barrier = Barrier(self.scale)

    async def __call__(self, messages):
        async for channel, message in self.run_threads(messages):
            yield channel, message

    async def run_threads(self, messages):
        asyncio.ensure_future(self.thread_feeder(messages))
        self.threads = dict((i, Thread(target=self.thread_consumer, args=(i,)).start())
                            for i in range(self.scale))
        queue_iter = aiter(self.oqueue.async_q.get, (None, pipe.EOT))
        async for channel, message in queue_iter:
            self.oqueue.async_q.task_done()
            yield channel, message

        # Empty queue of threads' EOTs
        for i in range(self.scale):
            if i:  # skip the one already consumed by aiter above
                await self.oqueue.async_q.get()
            self.oqueue.async_q.task_done()
            # print(f'thread {i} EOT received back')

        while self.threads:  # FIXME: join threads instead, and move or retire del self.threads[]
            asyncio.sleep(0.1)  # ... while at it, catch exceptions and clean up queue with task_done  # noqa: E501
        # print(f'all threads stopped')

    async def thread_feeder(self, messages):
        async for (channel, message) in messages:
            # TODO: recreate dead threads, report thread failures
            await self.iqueue.async_q.put((channel, message))
        for i in range(self.scale):
            await self.iqueue.async_q.put((None, pipe.EOT))
            # print(f'thread {i} EOT sent')

    def thread_consumer(self, threadnum):  # runs in thread
        # print(f'thread {threadnum} started')
        queue_iter = iter(self.iqueue.sync_q.get, (None, pipe.EOT))
        for channel, message in self.processor(queue_iter):
            self.iqueue.sync_q.task_done()
            self.oqueue.sync_q.put((channel, message))
        # print(f'thread {threadnum} EOT received')
        self.iqueue.sync_q.task_done()  # for EOT which does not make it past queue iterator
        # print(f'thread {threadnum} task done')
        self.barrier.wait()
        self.oqueue.sync_q.put((None, pipe.EOT))  # TODO: use threading.Event to send just one
        # print(f'thread {threadnum} EOT sent back')
        del self.threads[threadnum]
        # print(f'thread {threadnum} stopped')


class PriorityRegistry(dict):
    def ordered(self):
        return [item for _, item in sorted(self.items())]


class CmdRunner:

    async def runcmd(self, *args, raise_=True, **kwargs):
        try:
            proc = await asyncio.create_subprocess_exec(
                *map(str, args), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                **kwargs)
            await proc.wait()
            exitcode = await proc.wait()
            stdout = (await proc.stdout.read()).decode()
            stderr = (await proc.stderr.read()).decode()
            if exitcode:
                raise RuntimeError(f'Command {args} ({kwargs}) returned with exitcode {exitcode}, '
                                   f'stdout: {stdout or None}, stderr: {stderr or None}')

        except Exception as e:
            self.logger.exception(f'Error running command: {e}')
            if raise_:
                raise

        return exitcode, stdout, stderr
