# encoding: utf-8

import asyncio
import sys
import types
from collections import defaultdict
from concurrent import futures
from datetime import datetime
from queue import Queue
from threading import Thread

import aiozmq
import janus
import zmq
from async_generator import async_generator, yield_, yield_from_
from zmq.asyncio import Context

zcontext = Context.instance()

EOT = (None, b'EOT')


class LogUtil:
    def log(self, message):
        try:
            print('%s %s' % (self, message))
        except UnicodeError:
            print('%s %s' % (self, message.encode()))


class Runnable():
    def __init__(self, *args, **kwargs):
        self.active = kwargs.pop('active', True)

    def start(self, loop):
        self.log('Starting')
        self.active = True
        self.loop = loop
        return self.run()

    def stop(self):
        self.log('Stopping')
        self.active = False

    async def run(self):
        pass


class Pipe(Runnable, LogUtil):
    """Message transit mechanism."""
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


class DevNull(Pipe):
    def send(self, message, **kwargs):
        pass


class QueueIterator(LogUtil):
    def __init__(self, queue):
        self.queue = queue
        self.log('Initialized')

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self.queue.get()
        self.queue.task_done()
        if message == EOT:
            self.stop()
            await self.queue.join()
            raise StopAsyncIteration

        return message


class QueuePipe(Pipe, QueueIterator):
    def __init__(self, name, queue=None, maxsize=1, *args, **kwargs):
        Pipe.__init__(self, name, *args, **kwargs)
        QueueIterator.__init__(self, queue or asyncio.Queue(maxsize=maxsize))

    async def receive(self, wait=True):
        self.log('Receiving')
        if not wait:
            raise NotImplementedError
        try:
            return await self.__anext__()

        except StopAsyncIteration:
            return EOT

    @async_generator
    async def receiver(self):
        while self.active:
            self.log('Receiving+')
            await yield_(await self.receive())


class FilePipe(QueuePipe):
    def __init__(self, name, fileobj, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self._fileobj = fileobj

    async def run(self):
        self.loop.add_reader(self._fileobj.fileno(), self._reader)

    def _reader(self):
        line = self._fileobj.readline()
        if not line:  # EOF
            self.loop.remove_reader(self._fileobj.fileno())
            line = EOT
        asyncio.ensure_future(self.queue.put(line), loop=self.loop)


class ThreadPipe(QueuePipe):
    def __init__(self, name, queue=None, maxsize=1, *args, **kwargs):
        queue = queue or Queue(maxsize=maxsize)
        super().__init__(name, queue=queue, *args, **kwargs)


class TCPPipe(Pipe):
    def __init__(self, name, maxsize=1024**2, **kwargs):
        super().__init__(name, **kwargs)
        self.maxsize = maxsize

    async def run(self):
        host, port = (self.address.split(':') + [None])[:2]
        if not port:
            host, port = '0.0.0.0', host
        self._reader, self._writer = await asyncio.open_connection(
            host=host, port=port, loop=self.loop, limit=self.maxsize)
        self.log('TCP socket about to be created: %s' % self.address)

    async def _get(self, attr):
        attr = '_%s' % attr
        while self.active and not hasattr(self, attr):
            await asyncio.sleep(0.01)
        return getattr(self, attr)

    async def send(self, message, wait=False):
        # self.log('Sending')
        if wait:
            raise NotImplementedError

        writer = await self._get('writer')
        writer.write(message)

    async def receive(self, wait=True):
        # self.log('Receiving')
        reader = await self._get('reader')
        if not wait:
            raise NotImplementedError

        line = await reader.readline()
        if not line:  # EOF
            reader.feed_eof()
            self.stop()
            line = EOT
        else:
            line = line.decode()
        return line

    @async_generator
    async def receiver(self):
        reader = await self._get('reader')
        while self.active:
            # self.log('Receiving+')
            await yield_(await self.receive())


class PulsePipe(QueuePipe):
    def __init__(self, name, delay, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.delay = delay

    async def run(self):
        while self.active:
            await asyncio.sleep(self.delay)
            await self.queue.put(datetime.now())


class JanusPipe(Pipe):
    INPUT_IS_SYNC = b'INPUT_IS_SYNC'
    INPUT_IS_ASYNC = b'INPUT_IS_ASYNC'

    def __init__(self, name=None, mode=INPUT_IS_SYNC, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        assert mode in (self.INPUT_IS_ASYNC, self.INPUT_IS_SYNC)
        self._queue = janus.Queue(maxsize=1)
        self.receiver = QueueIterator(self._queue.async_q)
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
        # self.log('Receiving')
        return self._queue.sync_q.get(wait)

    async def receive(self, wait=True):
        # self.log('Receiving')
        if wait:
            return await self._queue.async_q.get()
        else:
            return await self._queue.async_q.get_nowait()

    def _sync_receiver(self):
        while self.active:
            # self.log('Receiving+')
            yield self._input.get()


class ZMQPipe(Pipe):
    PUB_SUB = b'PUB_SUB'
    PUSH_PULL = b'PUSH_PULL'
    SENDER = b'SENDER'
    RECEIVER = b'RECEIVER'

    def __init__(self, name, type_=None, mode=PUSH_PULL, **kwargs):
        super().__init__(name, **kwargs)
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

    async def run(self):
        self.log('Running')
        self._socket = await self._create_socket(
            self._modes[self.mode][self.type_]['mode'],
            self._modes[self.mode][self.type_]['method'])
        self.log('Socket [%s, %s] created: %s' %
                 (self.mode.decode(), self.type_.decode(), self.address))

    async def send(self, message, wait=False):
        self.log('Sending')
        if wait:
            raise NotImplementedError
        await self._socket.write((message,))

    @asyncio.coroutine
    def receive(self, wait=True):
        # self.log('Receiving')
        yield (yield from self._input.recv(0 if wait else zmq.NOBLOCK))

    @asyncio.coroutine
    def receiver(self):
        while self.active:
            # self.log('Receiving+')
            yield (yield from self._input.recv())


class SyncZMQPipe(Pipe):
    PUB_SUB = b'PUB_SUB'
    PUSH_PULL = b'PUSH_PULL'
    SENDER = b'SENDER'
    RECEIVER = b'RECEIVER'

    def __init__(self, name, type_=None, mode=PUSH_PULL, **kwargs):
        super().__init__(name, **kwargs)
        self.type_ = type_
        self.mode = mode

    _modes = {
        PUSH_PULL: {
            RECEIVER: dict(mode=zmq.PULL, method='connect'),
            SENDER: dict(mode=zmq.PUSH, method='bind')},
        PUB_SUB: {
            RECEIVER: dict(mode=zmq.SUB, method='connect'),
            SENDER: dict(mode=zmq.PUB, method='bind')}}

    @property
    def _create_socket(self, type_, mode):
        if not hasattr(self, '_isocket'):
            self._isocket = self.zcontext.socket(self._isocket_type)
            self._isocket.connect(self.address)
        return self._isocket

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


PIPETYPES = dict(file=FilePipe, janus=JanusPipe, null=DevNull, pulse=PulsePipe,
                 queue=ThreadPipe, zmq=ZMQPipe)


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
        self.collector = asyncio.Queue(maxsize=self.buffersize)
        coroutines = [pipe.start(self.loop) for pipe in self.channels.values()]
        if len(self.channels) > 1:
            for name in self.channels:
                coroutines.append(
                    self.feed(name, self.channels[name]))
                self.log('Created feed %s' % name)
            self._active_feeds = set(self.channels)
        return asyncio.gather(*coroutines)

    async def feed(self, name, pipe):
        while self.active:
            # self.log('Waiting for pipe %s' % name)
            # self.log('pipe.receive: %r' % pipe.receive)
            message = await pipe.receive()
            # self.log('Pipe %s gave %r' % (name, message))
            if message == EOT:
                self.log('Exhausted feed: %s' % name)
                self._active_feeds.remove(name)
                if len(self._active_feeds) == 0:
                    await self.collector.put((None, EOT))
                return

            else:
                await self.collector.put((name, message))
                # self.log('Collected %r' % message)

    @async_generator
    async def receiver(self):
        if len(self.channels) == 1:
            channel, pipe = list(self.channels.items())[0]
            async for message in pipe:
                if message == EOT:
                    self.stop()
                else:
                    await yield_((channel, message))
                if not self.active:
                    break

        else:
            while self.active:
                channel, message = await self.collector.get()
                if message is EOT:
                    self.stop()
                else:
                    await yield_((channel, message))


class Outbox(Manifold):
    def __call__(self, messages):
        return self.sender(messages)

    def run(self):
        coroutines = [pipe.start(self.loop) for pipe in self.channels.values()]
        return asyncio.gather(*coroutines)

    @async_generator
    async def sender(self, messages):
        self.log('Ready to send')
        async for channel, message in messages:
            self.log('Sending to %s: %r' % (channel, message))
            await self.channels[channel].send(message.encode())
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

    def start(self, *args):
        coroutines = [super().start(*args)]
        self.layers = ([self.inbox] +
                       self.ifilters.ordered() +
                       [self.spawn(self.processor)] +
                       self.ofilters.ordered() +
                       [self.outbox])
        for layer in self.layers:
            if isinstance(layer, Runnable):
                coroutines.append(layer.start(self.loop))
        return asyncio.gather(*coroutines)

    async def run(self):
        self.log('Spinning')
        while self.active and self.inbox.active:
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
    async def processor(self, messages):
        for channel, message in messages:
            channel, message = await self.process(channel, message)
            await yield_((channel, message))

    async def process(self, channel, message):
        raise NotImplementedError


class Filter(Runnable, LogUtil):
    def __call__(self, messages):
        return self.filter(messages)

    @async_generator
    async def filter(self, messages):
        async for channel, message in messages:
            channel, message = await self.process(channel, message)
            await yield_((channel, message))
            if not self.active:
                break

    async def process(self, channel, message):
        raise NotImplementedError


class Batcher(Filter):
    _RELEASE = b'.release'
    _DEFAULT = b'.default'

    def __init__(self, settings, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.settings = settings

    def initialize(self):
        self.batches = defaultdict(lambda: dict(data=list()))
        for key, settings in self.settings.items():
            self.batches[key].update(**settings)
            # TODO: populate self.keyfn based on settings
            self.keyfn = defaultdict(lambda: lambda msg: msg)

    def start(self):
        self.initialize()
        self.feed = asyncio.Queue(maxsize=1)
        coroutines = [super().start(self.loop), self.batcher()]
        for key, batch in self.batches.items():
            batch['alarm'] = asyncio.Queue(maxsize=1)
            coroutines.append(self.timer(key))
        return asyncio.gather(*coroutines)

    @async_generator
    async def filter(self, messages):
        self.log('Waiting for messages')
        await self.feed.put(messages)
        await self.feed.join()
        self.log('Listening for batch releases')
        while self.active:
            channel, message = await self.feed.get()
            self.log('Got batch')
            if channel == self._RELEASE:
                print(message)
                # channel, message = prepare(message)  # TODO
                channel, message = message['data'][0]
            print(channel, message)
            await yield_((channel, message))
            self.feed.task_done()

    async def batcher(self):
        messages = await self.feed.get()
        self.feed.task_done()
        self.log('Got messages')
        async for channel, message in messages:
            self.log('Got message')
            batch = self.batches[self.keyfn[channel](message)]
            if 'alarm' in batch:
                # TODO: handle optional deduplication
                batch['data'].append((channel, message))
                if len(batch['data']) == batch['maxsize']:
                    self.log('Releasing full batch')
                    await self.release(batch)
                elif len(batch['data']) == 1:
                    self.log('Setting alarm')
                    await batch['alarm'].put(self._DEFAULT)
            else:
                self.log('Unknown batch')
                await self.feed.put((channel, message))
        await self.feed.join()

    async def timer(self, key):
        from datetime import datetime
        batch = self.batches[key]
        while self.active:
            self.log('Timer %s ready' % key)
            delay = await batch['alarm'].get()
            if delay == self._DEFAULT:
                 delay = batch['maxtime']
            self.log('Timer %s starting (%s s)' % (key, delay))
            await asyncio.sleep(delay)
            self.log('Timer %s is up' % key)
            batch['alarm'].task_done()
            if batch['data']:
                self.log('Timer %s releasing batch' % key)
                await self.release(batch)
        await self.feed.join()

    async def release(self, batch):
        data = batch['data']
        batch['data'] = list()
        batch = batch.copy()
        batch['data'] = data
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
