#!/usr/bin/env python3

import logging
from datetime import datetime
from uuid import uuid4

from box import Box

_l = logging.getLogger(__name__)
stop = False


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
        setattr(self, '__setattr__', getattr(self, '__Xsetattr__'))  # prevent recursion

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