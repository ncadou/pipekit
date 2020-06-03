#!/usr/bin/env python3

import asyncio
import logging
from threading import Barrier, Thread

import janus

from .component import Component
from .pipe import Manifold, Message
from .utils import aiter

logger = logging.getLogger(__name__)


class Node(Component):
    """Processes messages."""

    def configure(self, process=None, scale=None, blocking=False,
                  inbox=None, ifilters=None, ofilters=None, outbox=None, **settings):
        if callable(process):
            self.process = process
        self.scale = int(scale or 1)
        self.blocking = self.__class__ is ThreadedNode or blocking
        self.inbox = self._join_pipes(inbox, Inbox)
        self.ifilters = ifilters or PriorityRegistry()
        self.ofilters = ofilters or PriorityRegistry()
        self.outbox = self._join_pipes(outbox, Outbox)
        self.layers = list()
        return settings

    def __str__(self):
        process_str = ''
        if not hasattr(self.process, 'stub') and not hasattr(self.process, 'overloaded'):
            process_str = f' {self.process}'
        return f'<{self.type} {self.id}{process_str}>'

    def _join_pipes(self, pipes, class_):
        if not isinstance(pipes, dict):
            pipes = dict(default=pipes)
        msgbox = self.workflow.make_component(class_, id=self.id, **pipes)
        for channel, pipe in pipes.items():
            pipe.parent = self
        return msgbox

    def start(self, *args):
        coroutines = [super().start(*args)]
        self.layers = ([self.inbox] +
                       self.ifilters.ordered() +
                       [self.spawn(self.processor)] +
                       self.ofilters.ordered() +
                       [self.outbox])
        for layer in self.layers:
            if isinstance(layer, Component):
                coroutines.append(layer.start())
        return asyncio.gather(*coroutines)

    async def run(self):
        await super().run()
        for layer in self.layers:
            if isinstance(layer, Component) and hasattr(layer, 'ready'):
                self.info(f'Waiting on layer {layer} to be ready')
                await layer.ready.wait()
                self.info(f'Layer {layer} is ready')
        if self.running and self.inbox.running:
            stack = self.layers[0]
            for layer in self.layers[1:]:
                stack = layer(stack)
            self.debug('Processing messages')
            try:
                async for result in stack:
                    result
            except Exception:
                self.exception(f'Node failure')
                raise

            self.debug(f'Finished processing')

    def spawn(self, processor):
        if self.blocking or self.scale > 1:
            processor = ThreadedProcessor(processor, scale=self.scale)
        return processor

    async def processor(self, messages):
        """Process and yield new (channel, message)."""
        async for channel, message in messages:
            yield await self.process(channel, message)

    async def process(self, channel, message):
        raise NotImplementedError(f'process() in {self}')

    process.stub = True

    def drop(self, message):
        raise NotImplementedError(f'drop() in {self}')


class Inbox(Manifold):

    def receive(self):
        raise NotImplementedError(f'receive() in {self}')

    async def receiver(self):
        self.debug('Receiving')
        for channel, pipe in self._active_channels():
            if not self.running:
                return

            try:
                message = await pipe.receive()
            except Exception:
                self.exception(f'Error while receiving on channel {channel}, {pipe}')
                raise

            self.debug(f'Got message from {channel}: {message}')
            if message == Message.EOT:
                self.debug('Exhausted channel: %s' % channel)
                pipe.stop()
            else:
                message.checkin(self)
                yield (channel, message)

        self.debug('Finished receiving')

    def _active_channels(self):
        active_channels = self.channels.copy()
        while active_channels:
            for channel, pipe in active_channels.copy().items():
                if not pipe.running:
                    self.debug(f'skipping stopped channel {channel}')
                    del active_channels[channel]
                else:
                    yield channel, pipe

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

            events.received.set()
        self.debug(f'Receiver for {channel} is exiting')

    async def _retry(self, channel, message, wait=0):
        await asyncio.sleep(wait)
        await self.channels[channel].send(message)

    def retry(self, channel, message, wait=0):
        self.debug(f'Retrying in {wait}s on {channel}: {message}')
        asyncio.run_coroutine_threadsafe(self._retry(channel, message, wait), loop=self.loop)


class Outbox(Manifold):
    def __call__(self, messages):
        return self.sender(messages)

    async def sender(self, messages):
        self.debug('Ready to send')
        async for channel, message in messages:
            channel = channel or 'default'
            if channel is Message.DROP:
                self.drop(message)
            else:
                self.debug(f'Sending to {channel}: {message}')
                if channel in self.channels:
                    await self.channels[channel].send(message)
                    self.debug(f'Sent to    {channel}: {message} [{self.channels[channel].id}]')
                    message.checkout(self)
                    yield channel, message

                    self.debug(f'Yielded    {channel}: {message}')
                else:
                    raise KeyError(f'Channel "{channel}" does not exist in {self}')

        self.debug('Finished sending')
        for pipe in self.channels.values():
         try:
            self.debug(f'EOT to {pipe}')
            await pipe.send(Message.EOT)
         except:
            __import__('pudb').set_trace()


class Filter(Component):
    def __call__(self, messages):
        return self.filter(messages)

    async def filter(self, messages):
        async for channel, message in messages:
            channel, message = await self.process(channel, message)
            yield (channel, message)

            if not self.running:
                break

    async def process(self, channel, message):
        raise NotImplementedError(f'process() out {self}')


class ChannelChain(Filter):

    TESTS = dict(directory=lambda m: m.path.id_dir(),
                 file=lambda m: m.path.id_file())

    async def process(self, channel, message):
        for test_name, test_fn in self.TESTS.items():
            if test_fn(message):
                return test_name, message


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
        queue_iter = aiter(self.oqueue.async_q.get, (None, Message.EOT))
        async for channel, message in queue_iter:
            self.oqueue.async_q.task_done()
            yield channel, message

        # Empty queue of threads' EOTs
        for i in range(self.scale):
            if i:  # skip the one already consumed by aiter above
                await self.oqueue.async_q.get()
            self.oqueue.async_q.task_done()

        while self.threads:
            asyncio.sleep(0.1)

    async def thread_feeder(self, messages):
        async for (channel, message) in messages:
            await self.iqueue.async_q.put((channel, message))
        for i in range(self.scale):
            await self.iqueue.async_q.put((None, Message.EOT))

    def thread_consumer(self, threadnum):  # runs in thread
        queue_iter = iter(self.iqueue.sync_q.get, (None, Message.EOT))
        for channel, message in self.processor(queue_iter):
            self.iqueue.sync_q.task_done()
            self.oqueue.sync_q.put((channel, message))
        self.iqueue.sync_q.task_done()  # for EOT which does not make it past queue iterator
        self.barrier.wait()
        self.oqueue.sync_q.put((None, Message.EOT))
        del self.threads[threadnum]


class PriorityRegistry(dict):
    def ordered(self):
        return [item for _, item in sorted(self.items())]
