import asyncio
import types
from collections import defaultdict
from concurrent import futures
from queue import Queue
from threading import Thread

import aiozmq
import janus
import zmq
from async_generator import async_generator, yield_, yield_from_
from zmq.asyncio import Context

EOT = '.EOT'

zcontext = Context.instance()


class LogUtil:
    def log(self, message):
        print('%s %s' % (self, message))


class Runnable():
    def __init__(self, *args, **kwargs):
        self.active = kwargs.pop('active', True)

    def start(self):
        self.log('Starting')
        self.active = True
        return self.run()

    def stop(self):
        self.log('Stopping')
        self.active = False

    async def run(self):
        pass


class Pipe(Runnable, LogUtil):
    """Message transit mechanism."""
    EOT = '__eot__'

    def __new__(cls, *args, **kwargs):
        cls = PIPETYPES.get(kwargs.get('impl'), cls)
        return super().__new__(cls)

    def __init__(self, name=None, impl=None, address=None, **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.impl = impl
        self.address = address
        self.log('Initialized')

    def __aiter__(self):
        return self.receiver()

    def __str__(self):
        return '<%s %r>' % (self.__class__.__name__, self.name)

    # def start(self):
    #     self.log('Starting')
    #     self.active = True
    #     if hasattr(self, '_start'):
    #         return self._start()

    # def stop(self):
    #     self.log('Stopping')
    #     self.active = False
    #     if hasattr(self, '_stop'):
    #         return self._stop()

    # @property
    # def run(self):
    #     return asyncio.coroutine(lambda: None)


class ThreadPipe(Pipe):
    @property
    def _input(self):
        if not hasattr(self, '_queue'):
            self._queue = Queue(maxsize=1)
        return self._queue

    @property
    def _output(self):
        return self._input

    def send(self, message, wait=True):
        self.log('Sending')
        self._output.put(message, wait)

    def receive(self, wait=True):
        self.log('Receiving')
        return self._input.get(wait)

    def receiver(self):
        while self.active:
            self.log('Receiving+')
            yield self._input.get()


class JanusPipe(Pipe):
    INPUT_IS_SYNC = '.INPUT_IS_SYNC'
    INPUT_IS_ASYNC = '.INPUT_IS_ASYNC'

    def __init__(self, name=None, mode=INPUT_IS_SYNC, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        assert mode in (self.INPUT_IS_ASYNC, self.INPUT_IS_SYNC)
        self._queue = janus.Queue(maxsize=1)
        self.receiver = AsyncQueueIterator(self._queue.async_q)
        if mode == self.INPUT_IS_ASYNC:
            self.send, self.receive, self.receiver = (
                self._async_send, self._sync_receive, self._sync_receiver)

    async def _async_send(self, message, wait=True):
        self.log('Sending')
        if wait:
            await self._queue.async_q.put(message)
        else:
            await self._queue.async_q.put_nowait(message)

    def send(self, message, wait=True):
        self.log('Sending')
        self._queue.sync_q.put(message, wait)

    def _sync_receive(self, wait=True):
        self.log('Receiving')
        return self._queue.sync_q.get(wait)

    async def receive(self, wait=True):
        self.log('Receiving')
        if wait:
            return await self._queue.async_q.get()
        else:
            return await self._queue.async_q.get_nowait()

    def _sync_receiver(self):
        while self.active:
            self.log('Receiving+')
            yield self._input.get()


class ZMQPipe(Pipe):
    PUB_SUB = '.PUB_SUB'
    PUSH_PULL = '.PUSH_PULL'
    SENDER = '.SENDER'
    RECEIVER = '.RECEIVER'

    def __init__(self, name, zcontext=zcontext, type_=None, mode=PUSH_PULL,
                 **kwargs):
        super().__init__(name, **kwargs)
        self.zcontext = zcontext
        self.type_ = type_
        self.mode = mode

    _modes = {
        PUSH_PULL: {
            RECEIVER: dict(mode=zmq.PULL, method='connect'),
            SENDER: dict(mode=zmq.PUSH, method='bind')},
        PUB_SUB: {
            RECEIVER: dict(mode=zmq.SUB, method='connect'),
            SENDER: dict(mode=zmq.PUB, method='bind')}}

    @asyncio.coroutine
    def _create_socket(self, type_, mode):
        return (yield from aiozmq.create_zmq_stream(
            type_, **{mode: self.address}))
        # high_read, high_write, low_read, low_write

    async def run(self):
        self._socket = await self._create_socket(
            self._modes[self.mode][self.type_]['mode'],
            self._modes[self.mode][self.type_]['method'])
        self.log('Socket [%s, %s] created' % (self.mode, self.type_))

    async def send(self, message, wait=False):
        self.log('Sending')
        if wait:
            raise NotImplementedError
        self._socket.write((message,))

    @asyncio.coroutine
    def receive(self, wait=True):
        self.log('Receiving')
        yield (yield from self._input.recv(0 if wait else zmq.NOBLOCK))

    @asyncio.coroutine
    def receiver(self):
        while self.active:
            self.log('Receiving+')
            yield (yield from self._input.recv())


class PyZMQPipe(Pipe):
    PUB_SUB = '.PUB_SUB'
    PUSH_PULL = '.PUSH_PULL'

    def __init__(self, name, zcontext=zcontext, mode=PUSH_PULL, **kwargs):
        super().__init__(name, **kwargs)
        self.zcontext = zcontext
        if mode == self.PUSH_PULL:
            self._isocket_type = zmq.PULL
            self._osocket_type = zmq.PUSH
        elif mode == self.PUB_SUB:
            self._isocket_type = zmq.SUB
            self._osocket_type = zmq.PUB
        else:
            raise ValueError('Unknown mode %r' % mode)

    @property
    def _input(self):
        if not hasattr(self, '_isocket'):
            self._isocket = self.zcontext.socket(self._isocket_type)
            self._isocket.connect(self.address)
        return self._isocket

    @property
    def _output(self):
        if not hasattr(self, '_osocket'):
            self._osocket = self.zcontext.socket(self._osocket_type)
            self._osocket.bind(self.address)
        return self._osocket

    async def send(self, message, wait=True):
        self.log('Sending')
        await self._output.send(message, 0 if wait else zmq.NOBLOCK)

    @asyncio.coroutine
    def receive(self, wait=True):
        self.log('Receiving')
        yield (yield from self._input.recv(0 if wait else zmq.NOBLOCK))

    @asyncio.coroutine
    def receiver(self):
        while self.active:
            self.log('Receiving+')
            yield (yield from self._input.recv())


class DevNull(Pipe):
    def send(self, message, **kwargs):
        pass


PIPETYPES = dict(janus=JanusPipe, queue=ThreadPipe, zmq=ZMQPipe, null=DevNull)


class AsyncQueueIterator(LogUtil):
    def __init__(self, queue):
        self.queue = queue
        self.log('Initialized')

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.log('Waiting')
        message = await self.queue.get()
        self.log('Got %r' % (message,))
        self.queue.task_done()
        if message == EOT:
            self.count -= 1
            if self.count == 0:
                await self.queue.join()
                raise StopAsyncIteration
        self.log('Returning %r' % (message,))
        return message
        if not self.manifold.active:
            raise StopAsyncIteration


class Collector(AsyncQueueIterator):
    def __init__(self, manifold):
        self.manifold = manifold
        self.count = len(manifold.channels)
        self.name = '%s-collector' % manifold.name
        super().__init__(manifold.collector)


class Manifold(Pipe):
    def __init__(self, name='manifold', buffersize=1, **channels):
        self.buffersize = buffersize
        self.channels = channels
        super().__init__(name)

    def __str__(self):
        return '<%s [%s]>' % (self.__class__.__name__,
                              ', '.join('%s:%s' % (n, p.name)
                                        for n, p in self.channels.items()))


class Inbox(Manifold):
    def run(self):
        feeds = dict()
        self.collector = asyncio.Queue(maxsize=self.buffersize)
        for name in self.channels:
            feeds[name] = self.feed(name, self.channels[name], self.collector)
            self.log('Created feed %s' % name)
        runnables = [pipe.start() for pipe in self.channels.values()]
        runnables += feeds.values()
        print(repr(runnables))
        return asyncio.gather(*runnables)

    # async def feed(self, name, pipe, collector):
    #     while self.active:
    #         self.log('Waiting for pipe %s' % name)
    #         await collector.put((name, await pipe.receive()))

    async def feed(self, name, pipe, collector):
        message = None
        while self.active and message != EOT:
            self.log('Waiting for pipe %s' % name)
            self.log('pipe.receive: %r' % pipe.receive)
            message = await pipe.receive()
            self.log('Pipe %s gave %r' % (name, message))
            await collector.put((name, message))
            self.log('Sent %r to collector' % message)
        self.log('Exiting')

    # # @types.coroutine
    # def receive(self):
    #     self.log('Waiting for collector')
    #     return (yield from self.collector.get())

    # # @types.coroutine
    # def receiver(self):
    #     while self.active:
    #         self.log('Waiting for collector+')
    #         get = self.collector.get()
    #         print(get.__class__)
    #         print(get.func)
    #         print(asyncio.isfuture(get))
    #         print(asyncio.isfuture(get.func))
    #         print(next(get))
    #         fut = asyncio.wait(get, return_when=futures.ALL_COMPLETED)
    #         print(repr(fut))
    #         1/0
    #         # message = (yield from get)
    #         self.log('Collector yielded %s' % message)
    #         yield message

    def receiver(self):
        return Collector(self)


class Outbox(Manifold):
    def run(self):
        runnables = [pipe.start() for pipe in self.channels.values()]
        print(repr(runnables))
        return asyncio.gather(*runnables)

    @async_generator
    async def send(self, messages):
        self.log('Ready to send')
        async for channel, message in messages:
            self.log('Sending to %s' % channel)
            await self.channels[channel].send(message)
            await yield_((channel, message))


class Node(Runnable, LogUtil):
    """Processes messages."""
    def __init__(self, name, process=None, scale=1, active=True,
                 inbox=None, ifilters=None, ofilters=None, outbox=None):
        self.name = name
        self.log('Initializing')
        if callable(process):
            self.process = process
        self.scale = scale
        self.active = active
        self.inbox = self._join(inbox, Inbox)
        self.ifilters = ifilters or PriorityRegistry()
        self.ofilters = ofilters or PriorityRegistry()
        self.outbox = self._join(outbox, Outbox)
        self.layers = list()
        self.log('Initialized')

    def __str__(self):
        return '<%s %r>' % (self.__class__.__name__, self.name)

    def _join(self, pipe, class_):
        if callable(getattr(pipe, 'items', None)):
            self.log('Wrapping %r in %s' % (pipe, class_))
            pipe = class_(**pipe)
        elif not isinstance(pipe, Manifold):
            self.log('Wrapping %r in single %s' % (pipe, class_))
            pipe = class_(default=pipe)
        return pipe

    def start(self):
        self.layers = ([self.inbox] +
                       self.ifilters.ordered() +
                       [self.spawn(self.processor)] +
                       self.ofilters.ordered() +
                       [self.outbox.send])
        runnables = [super().start()]
        for layer in self.layers:
            if isinstance(layer, Runnable):
                runnables.append(layer.start())
        return asyncio.gather(*runnables)

    async def run(self):
        self.log('Spinning')
        while self.active:
            chain = self.layers[0]
            for layer in self.layers[1:]:
                chain = layer(chain)
            self.log('Consuming messages')
            async for result in chain:
                pass

    def spawn(self, processor):
        if self.scale == 1:
            return processor
        raise NotImplementedError

    @async_generator
    def processor(self, messages):
        for channel, message in messages:
            channel, message = await self.process(channel, message)
            await yield_((channel, message))

    async def process(self, channel, message):
        raise NotImplementedError


class Filter(Runnable, LogUtil):
    def __call__(self, messages):
        return self.filter(messages)

    @async_generator
    def filter(self, messages):
        for message in messages:
            raise NotImplementedError
            await yield_(message)


class Batcher(Filter):
    _RELEASE = '.release'

    def initialize(self):
        self.batches = defaultdict(lambda: dict(data=list()))
        # TODO: populate self.batches based on settings

    def start(self):
        self.initialize()
        self.feed = asyncio.Queue(maxsize=1)
        runnables = [super().start(), self.feeder()]
        for key, batch in self.batches.iteritems():
            batch['alarm'] = asyncio.Queue(maxsize=1)
            runnables.append(self.timer(key))
        return asyncio.gather(*runnables)

    @async_generator
    def filter(self, messages):
        await self.feed.put(messages)
        await self.feed.join()
        while self.active:
            channel, message = await self.feed.get()
            if channel == self._RELEASE:
                channel, message = prepare(message)  # TODO
            await yield_(channel, message)
            self.feed.task_done()

    async def feeder(self):
        messages = await self.feed.get()
        self.feed.task_done()
        async for channel, message in messages:
            batch = self.batches[makekey(channel, message)]  # TODO
            if 'settings' in batch:
                # TODO: handle optional deduplication
                batch['data'].append((channel, message))
                if len(batch['data']) == batch['maxsize']:
                    await self.release(batch)
                elif len(batch['data']) == 1:
                    await batch['alarm'].put(None)
            else:
                await self.feed.put((channel, message))
        await self.feed.join()

    async def timer(self, key):
        batch = self.batches[key]
        while self.active:
            delay = await batch['alarm'].get() or batch['maxtime']
            asyncio.sleep(delay)
            batch['alarm'].task_done()
            if batch['data']:
                await self.release(batch)
        await self.feed.join()

    async def release(self, batch):
        data = batch['data']
        batch['data'] = list()
        batch = batch.copy()
        batch.update(data=data)
        await self.feed.put((self._RELEASE, batch))


class Joiner(LogUtil):
    """Join protocol:

    - Create a join id.
    - Mark all sent messages with the id.
    - Send last message with count.

    """
    def __init__(self, node):
        self.node = node
        self.groups = defaultdict(dict)
        self.totals = defaultdict(lambda: None)
        self.log('Initialized')

    def __call__(self, messages):
        for prefix, message in messages:
            op = message.checkpoints.get(self.node.name)
            if op and op.type == 'join':
                self.groups[op.id][message.id] = message
                if op.total:
                    self.totals[op.id] = op.total
                total = self.totals[op.id]
                if total is not None:
                    if len(self.groups[op.id]) == total:
                        yield self.merge(op.id)
                    else:
                        continue
            else:
                yield prefix, message


class PriorityRegistry(dict):
    def ordered(self):
        return [item for _, item in sorted(self.items())]
