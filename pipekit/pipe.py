#!/usr/bin/env python3

import asyncio
import logging
from datetime import datetime
from uuid import uuid4

from box import Box

from .component import Component
from .utils import aiter

_l = logging.getLogger(__name__)


class Message:
    """Data wrapper."""

    _instances = dict()  # TODO: check at end of workflow if any undropped messages are remaining

    DROP = object()

    def __init__(self, meta=None, **data):
        if not meta:
            meta = dict()
        meta.setdefault('id', uuid4())
        meta = Box(meta or {},
                   created=datetime.utcnow(),
                   history=list(),
                   dropped=False)
        self._instances[meta.id] = self
        self.__data = Box(data, meta=meta.to_dict(), box_dots=True)
        setattr(self, f'__setattr__', getattr(self, f'__Xsetattr__'))  # prevent recursion

    def __getattr__(self, name, **kwargs):
        return getattr(self.__data, name, **kwargs)

    def __Xsetattr__(self, name, value):
        return setattr(self.__data, name, value)

    def __getitem__(self, name, **kwargs):
        return self.__data.get(name, **kwargs)

    def __setitem__(self, name, value):
        self.__data[name] = value

    def __str__(self):
        return f'<{self.__class__.__name__} {self.meta.id}>'

    def checkin(self, component):
        self._tag(component, 'in')

    def checkout(self, component):
        self._tag(component, 'out')

    @property
    def dropped(self):
        return self.meta.dropped

    def drop(self, component):
        self._tag(component, 'dropped')
        del self._instances[self.meta.id]
        self.meta.dropped = True
        component.debug(f'Dropped message: {self}')

    def _tag(self, component, tag):
        self.meta.history.append((datetime.utcnow(), component.id, tag))

    class __EOT__:
        """Used to signal the end of transmission of a queue."""

        def __repr__(self):
            return str(self)

        def __str__(self):
            return '__EOT__'

        def __bytes__(self):
            return b'__EOT__'

        def __eq__(self, other):
            if isinstance(other, bytes):
                return self.__bytes__() == other

            elif isinstance(other, str):
                return self.__str__() == other

            else:
                return self is other

    EOT = __EOT__()


class Pipe(Component):
    """Message transit mechanism."""

    def __aiter__(self):
        return self.receiver()

    def __str__(self):
        return f'<{self.__class__.__name__} {self.id}>'

    async def send(self, message, **kwargs):
        raise NotImplementedError(f'send() out {self}')

    async def sender(self, messages):
        raise NotImplementedError(f'sender() out {self}')

        yield

    def drop(self, message):
        if isinstance(message, Message):
            message.drop(self)

    async def receive(self, **kwargs):
        raise NotImplementedError(f'receive() out {self}')

    async def receiver(self):
        raise NotImplementedError(f'receiver() out {self}')

        yield


class PipeRef:
    """Holds a node channel reference that can later on be resolved to the actual pipe object."""

    def __init__(self, node, msgbox, channel):
        self.node = node
        self.msgbox = msgbox
        self.channel = channel

    def __str__(self):
        return f'<{type(self).__name__} {self.node.key}.{self.msgbox}:{self.channel}>'

    def __repr__(self):
        return str(self)

    def resolve(self):
        return self.node[self.msgbox][self.channel].instance


class Manifold(Pipe):
    def configure(self, buffersize=1, **channels):
        self.buffersize = buffersize
        self.channels = channels
        self.ready = asyncio.Event()
        return channels

    def __str__(self):
        return '<%s [%s]>' % (self.__class__.__name__,
                              ', '.join('%s:%s' % (n, p.id)
                                        for n, p in self.channels.items()))

    async def run(self):
        await super().run()
        self.debug(f'- starting pipes')
        for pipe in self.channels.values():
            if not pipe.running:
                await pipe.start()
        self.ready.set()


class NullPipe(Pipe):
    async def send(self, message, **kwargs):
        self.drop(message)

    async def receive(self):
        return self.running and await asyncio.sleep(1 / self.settings.get('rate', 10 ** 0))

    async def receiver(self):
        while await self.receive() or self.running:
            yield


class DataPipe(Pipe):
    """Outputs preloaded messages, in order."""

    def configure(self, messages=None):
        self.messages = [Message(**m) for m in reversed(messages or [])]
        return dict(messages=len(self.messages))

    async def receive(self):
        try:
            return self.messages.pop(-1)

        except IndexError:
            return Message.EOT

    async def receiver(self):
        while self.running:
            try:
                yield await self.receive()

            except IndexError:
                break


class QueuePipe(Pipe):
    def configure(self, queue=None, maxsize=1):
        self._queue = queue or asyncio.Queue(maxsize=maxsize)

    async def send(self, *args, **kwargs):
        return await self._queue.put(*args, **kwargs)

    async def receive(self):
        if not self.running:
            return Message.EOT

        message = await self._queue.get()
        self._queue.task_done()
        return message

    async def receiver(self):
        return aiter(self.receive, Message.EOT)
