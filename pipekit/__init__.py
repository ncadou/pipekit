from collections import defaultdict
from queue import Queue

import zmq


class Pipe:
    """Message transit mechanism."""
    def __init__(self, name, location=None):
        self.name = name
        self.location = location
        self.active = True

    def __iter__(self):
        return self.receiver()

    def start(self):
        self.active = True

    def stop(self):
        self.active = False


class ThreadPipe(Pipe):
    @property
    def _input(self):
        if self._queue is None:
            self._queue = Queue()
        return self._queue

    @property
    def _output(self):
        return self._input

    def send(self, message):
        self._queue.put(message)

    def receive(self, wait=True):
        return self._queue.get(wait)

    def receiver(self):
        while self.active:
            yield self._queue.get()


class ZMQPipe(Pipe):
    def __init__(self, name, context, **kwargs):
        super().__init__(name, **kwargs)
        self.context = context

    @property
    def _input(self):
        if self.__input is None:
            self.__input = self.context.socket(zmq.PUSH)
            self.__input.bind(self.location)
        return self.__input

    @property
    def _output(self):
        if self.__output is None:
            self.__output = self.context.socket(zmq.PULL)
            self.__output.connect(self.location)
        return self.__output

    def send(self, message):
        self._output.send(message)

    def receive(self, wait=True):
        return self._input.recv(0 if wait else zmq.NOBLOCK)

    def receiver(self):
        while self.active:
            yield self._input.recv()


class Message:
    """Represents a data unit moving through the system."""


class Builder:
    """Connects nodes using pipes."""


class Manager:
    """Uses builders to create and morph a topology."""


class Node:
    """Processes messages."""
    def __init__(self, name, process=None, scale=1, active=True,
                 iqueue=None, ifilters=None, ofilters=None, oqueue=None):
        self.name = name
        self.scale = scale
        self.active = active
        self.iqueue = iqueue
        self.ifilters = ifilters or PriorityRegistry()
        self.ofilters = ofilters or PriorityRegistry()
        self.oqueue = oqueue
        self.layers = list()
        if callable(process):
            self.process = process

    def start(self, cqueue):
        """Start control channel listener."""
        spawn(self.listener, cqueue)

    def run(self):
        while self.active:
            self.layers = ([self.iqueue.recv] +
                           self.ifilters.ordered() +
                           self.multiply(self.processor) +
                           self.ofilters.ordered() +
                           [self.oqueue.send])
            messages = self.layers[0]()
            for layer in self.layers[1:]:
                messages = layer(messages)
            for result in messages:
                self.acton(result)

    def scale(self, processor):
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
