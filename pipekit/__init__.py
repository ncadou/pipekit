from collections import defaultdict
from queue import Queue

import zmq

context = zmq.Context()


class Pipe:
    """Message transit mechanism."""
    def __new__(cls, *args, **kwargs):
        cls = PIPE_IMPLEMENTATIONS.get(kwargs.get('impl'), cls)
        return super().__new__(cls)

    def __init__(self, name, impl=None, active=True, address=None):
        self.name = name
        self.impl = impl
        self.active = active
        self.address = address

    def __iter__(self):
        return self.receiver()

    def start(self):
        self.active = True

    def stop(self):
        self.active = False


class ThreadPipe(Pipe):
    @property
    def _input(self):
        if not hasattr(self, '_queue'):
            self._queue = Queue()
        return self._queue

    @property
    def _output(self):
        return self._input

    def send(self, message, wait=True):
        self._output.put(message, wait)

    def receive(self, wait=True):
        return self._input.get(wait)

    def receiver(self):
        while self.active:
            yield self._input.get()


class ZMQPipe(Pipe):
    def __init__(self, name, context=context, **kwargs):
        super().__init__(name, **kwargs)
        self.context = context

    @property
    def _input(self):
        if not hasattr(self, '_isocket'):
            self._isocket = self.context.socket(zmq.PULL)
            self._isocket.connect(self.address)
        return self._isocket

    @property
    def _output(self):
        if not hasattr(self, '_osocket'):
            self._osocket = self.context.socket(zmq.PUSH)
            self._osocket.bind(self.address)
        return self._osocket

    def send(self, message, wait=True):
        self._output.send(message, 0 if wait else zmq.NOBLOCK)

    def receive(self, wait=True):
        return self._input.recv(0 if wait else zmq.NOBLOCK)

    def receiver(self):
        while self.active:
            yield self._input.recv()


PIPE_IMPLEMENTATIONS = dict(queue=ThreadPipe, zmq=ZMQPipe)


class Message:
    """Represents a data unit moving through the system."""


class Builder:
    """Connects nodes using pipes."""


class Manager:
    """Uses builders to create and morph a topology."""


class Node:
    """Processes messages."""
    def __init__(self, name, process=None, scale=1, active=True,
                 inbox=None, ifilters=None, ofilters=None, outbox=None):
        self.name = name
        self.scale = scale
        self.active = active
        self.inbox = inbox
        self.ifilters = ifilters or PriorityRegistry()
        self.ofilters = ofilters or PriorityRegistry()
        self.outbox = outbox
        self.layers = list()
        if callable(process):
            self.process = process

    def reader(self):
        while self.active:
            for msg in self.inbox:
                yield 'default', msg

    def writer(self, inbox):
        while self.active:
            for channel, msg in inbox:
                self.outbox.send(msg)
                yield channel, msg

    def run(self):
        while self.active:
            self.layers = ([self.reader] +
                           self.ifilters.ordered() +
                           [self.spawn(self.processor)] +
                           self.ofilters.ordered() +
                           [self.writer])
            messages = self.layers[0]()
            for layer in self.layers[1:]:
                messages = layer(messages)
            for result in messages:
                self.acton(result)

    def spawn(self, processor):
        if self.scale == 1:
            return processor
        raise NotImplementedError

    def processor(self, inbox):
        for channel, msg in inbox:
            channel, msg = self.process(channel, msg)
            yield channel, msg

    def process(self, channel, msg):
        raise NotImplementedError

    def acton(self, result):
        pass


class Joiner:
    """Join protocol:

    - Create a join id.
    - Mark all sent messages with the id.
    - Send last message with count.

    """
    def __init__(self, node):
        self.node = node
        self.groups = defaultdict(dict)
        self.totals = defaultdict(lambda: None)

    def __call__(self, messages):
        for prefix, msg in messages:
            op = msg.checkpoints.get(self.node.name)
            if op and op.type == 'join':
                self.groups[op.id][msg.id] = msg
                if op.total:
                    self.totals[op.id] = op.total
                total = self.totals[op.id]
                if total is not None:
                    if len(self.groups[op.id]) == total:
                        yield self.merge(op.id)
                    else:
                        continue
            else:
                yield prefix, msg


class Channels:
    def __init__(self, channels):
        self.channels = channels

    def feed(self, name, outbox):
        for msg in self.channels[name].recv():
            outbox.send((name, msg))

    def recv(self):
        outbox = Pipe()
        for name in self.channels:
            feeds[name] = spawn(self.feed, name, outbox)
        for channel, msg in outbox.recv():
            yield channel, msg
        for name in self.channels:
            feeds[name].join()

    def send(self, channel, msg):
        self.channels[channel].send(msg)
        yield channel, msg


class PriorityRegistry(dict):
    def ordered(self):
        return [item for _, item in sorted(self.items())]
