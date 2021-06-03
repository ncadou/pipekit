#!/usr/bin/env python3

import asyncio
import logging
from collections import defaultdict
from functools import partial

from box import Box

_l = logging.getLogger(__name__)

_events = defaultdict(asyncio.Event)


class Component:
    """An element in a workflow that can be configured, run, and uniquely named."""

    __instances = dict()

    def __init__(self, workflow, *args, id=None, parent=None, logger=_l, **kwargs):
        self.workflow = workflow
        self.id = id
        if id:
            key = (type(self), id)
            if key in self.__instances:
                raise ValueError(f'{type(self).__name__} with ID "{id}" '
                                 f'already exists ({self.__instances[key]})')

            self.__instances[key] = self
        self.parent = parent
        self.children = list()
        if parent:
            parent.children.append(self)
        self.logger = logger

        self.loop = asyncio.get_event_loop()

        self._running = False
        self._paused = False

        self._settings = Box(self.configure(*args, **kwargs) or dict())
        config_text = (' '.join(f'{k}={v}'
                                for k, v in workflow.safe_settings(self._settings).items()))
        self.debug(f'Initialized {config_text}')

    def configure(self, **settings):
        return settings

    def settings(self, **settings):
        return Box(self._settings, **settings)

    @property
    def running(self):
        return self._running

    @property
    def type(self):
        return type(self).__name__

    def start(self):
        self.debug('Starting')
        self._running = True
        self._paused = False
        return self.run()

    def stop(self):
        self.debug('Stopping')
        self._running = False
        self._paused = False

    def pause(self):
        self.debug('Pausing')
        self._paused = True

    def resume(self):
        self.debug('Resuming')
        self._paused = False

    async def run(self):
        self.debug('Running')

    def _formatted(self, msg, *args):
        prefix = f'{self.id} ' if self.id else ''
        msg = f'{prefix}({self.type}) {msg}'
        return (msg,) + args

    def __getattr__(self, name):
        if name not in ('critical', 'error', 'warning', 'info', 'debug', 'exception'):
            raise AttributeError(f"'{self.type}' object has no attribute '{name}'")

        return partial(self._proxied_attr, name)

    def _proxied_attr(self, method, *args, **kwargs):
        if method == 'debug':
            if not ('all' in self.workflow.settings.logging.debug or
                    self.type in self.workflow.settings.logging.debug or
                    (self.id in self.workflow.settings.logging.debug)):
                return lambda *a, **kw: None

        return getattr(self.logger, method)(*self._formatted(*args), **kwargs)


class Evented:
    """Mixin providing activity status and events utilities."""

    _queues = list()
    _callbacks = defaultdict(list)

    @property
    def status(self):
        return getattr(self, '_status', None)

    @status.setter
    def status(self, event):
        fullevent = self.fullevent(event)
        if self.hasstatus(event):
            return

        if event in ['finished', 'exited']:
            evented = list(filter(lambda c: isinstance(c, Evented), self.children))
            if len(evented) > len(list(filter(lambda c: c.hasstatus(event), evented))):
                return

        self.debug(f'Emitting event {fullevent}')
        self._status = event
        _events[fullevent].set()
        for queue in self._queues:
            queue.put_nowait(fullevent)
        if isinstance(self.parent, Evented):
            self.parent.status = event
        for callback in self._callbacks[fullevent]:
            asyncio.ensure_future(callback())
        self._callbacks[fullevent].clear()

    def fullevent(self, event):
        return f'{self.id}:{event}'

    def hasstatus(self, event):
        """Return `True` if given event happened."""
        return _events[self.fullevent(event)].is_set()

    async def waiton(self, event):
        self.debug(f'Waiting on event {event}')
        await _events[event].wait()
        self.debug(f'Received event {event}')

    def getlistener(self):
        """Return a new `Queue` object that will be sent all events."""
        queue = asyncio.Queue()
        self._queues.append(queue)
        return queue

    def addcallback(self, event, callable, *args, **kwargs):
        """Register a callback that will be called upon the given event."""
        self._callbacks[event].append(partial(callable, *args, **kwargs))

    @property
    def running(self):
        """Return `True` if in one of the running states."""
        for status in ['started', 'running']:
            if self.hasstatus(status):
                return True

    @property
    def stopped(self):
        """Return `True` if in one of the stopped states."""
        for status in ['aborted', 'finished']:
            if self.hasstatus(status):
                return True
