#!/usr/bin/env python3

import asyncio
from collections.abc import Mapping, Sequence
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial

__MISSING__ = object()


class ThreadRunner:
    """Async wrapper for file-like objects that operate in normal, blocking mode."""

    def __init__(self, loop=None, executor=None, max_workers=4):
        self.loop = loop or asyncio.get_event_loop()
        self.executor = executor or ThreadPoolExecutor(max_workers)

    async def _to_thread(self, callable_, *args, **kwargs):
        """Run the provided callable in a thread and return the result."""
        return await self.loop.run_in_executor(
            self.executor, partial(callable_, *args, **kwargs))


class AsyncFile(ThreadRunner):
    """Async wrapper for file-like objects that operate in normal, blocking mode."""

    def __init__(self, fileobj=None, opener=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fileobj = fileobj
        self.opener = opener
        if not (fileobj or opener):
            raise ValueError('Need to be provided with either fileobj or opener')

    async def open(self, *args, **kwargs):
        if self.fileobj:
            raise RuntimeError('AsyncFile is already opened')

        self.fileobj = await self._to_thread(self.opener, *args, **kwargs)
        return self

    async def _proxy(self, name, *args, **kwargs):
        return await self._to_thread(getattr(self.fileobj, name), *args, **kwargs)

    async def write(self, *args):
        return await self._proxy('write', *args)

    async def read(self, *args):
        return await self._proxy('read', *args)

    async def close(self):
        return await self._proxy('close')

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


class AsyncRunner:

    def __init__(self, asyncobj, loop=None, timeout=None):
        self.asyncobj = asyncobj
        self.loop = loop or asyncio.get_event_loop()
        self.timeout = timeout

    def _to_async(self, name, *args, timeout=None, **kwargs):
        future = asyncio.run_coroutine_threadsafe(
            getattr(self.asyncobj, name)(*args, **kwargs), self.loop)
        return future.result(timeout or self.timeout)


class AsyncFileThreadWrapper(AsyncRunner):

    def write(self, *args, **kwargs):
        return self._to_async('write', *args, **kwargs)

    def read(self, *args, **kwargs):
        return self._to_async('read', *args, **kwargs)

    def close(self):
        return self._to_async('close')


class StreamedResponseFile(ThreadRunner):
    def __init__(self, opener, *args, chunk_size=2 ** 16, **kwargs):
        super().__init__(*args, **kwargs)
        self.opener = opener
        self.chunk_size = chunk_size

    async def open(self, *args, chunk_size=None, **kwargs):
        self.response = await self.opener(*args, **kwargs)
        self._iter_chunks = await self._to_thread(
            self.response.iter_content, chunk_size=chunk_size or self.chunk_size)
        return self

    async def read(self, *args, **kwargs):
        def get_chunk():
            for chunk in self._iter_chunks:
                return chunk

            return b''

        return await self._to_thread(get_chunk)

    def writable(self):
        return False

    async def close(self):
        self.response.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


async def aiter(object, sentinel=__MISSING__):
    """Async version of the iter() built-in."""
    if sentinel is __MISSING__:
        async for item in object:
            yield item

    else:
        while True:
            try:
                item = await object()
            except StopAsyncIteration:
                break

            if item == sentinel:
                break

            yield item


def isstr(obj):
    """Return True if the obj is a string of some sort."""
    return isinstance(obj, str)


def isdict(obj):
    """Return True if the obj is a mapping of some sort."""
    return isinstance(obj, Mapping)


def islist(obj):
    """Return True if the obj is a sequence of some sort."""
    return isinstance(obj, Sequence) and not isstr(obj)
