#!/usr/bin/env python3

from collections.abc import Mapping, Sequence

__MISSING__ = object()


async def aiter(object, sentinel=__MISSING__):
    """Async version of the `iter()` built-in."""
    if sentinel is __MISSING__:
        async for item in object:
            yield item

    else:
        while True:
            item = await object()
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
