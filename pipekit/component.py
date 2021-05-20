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

    def __init__(self, workflow, *args, id=None, conditions=None, logger=_l, **kwargs):
        self.workflow = workflow
        self.id = id
        if id:
            key = (type(self), id)
            if key in self.__instances:
                raise ValueError(f'{type(self).__name__} with ID "{id}" '
                                 f'already exists ({self.__instances[key]})')

            self.__instances[key] = self
        self.conditions = conditions or []
        self.logger = logger

        self.parent = None
        self.loop = asyncio.get_event_loop()

        self._running = False
        self._paused = False

        self._settings = Box(self.configure(**kwargs) or dict())
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

    def setevent(self, event):
        self.debug(f'Setting event {self.id}:{event}')
        _events[f'{self.id}:{event}'].set()

    async def waiton(self, event):
        self.debug(f'Waiting on event {event}')
        await _events[event].wait()
        self.debug(f'Received event {event}')

    def start(self):
        self._running = True
        self._paused = False
        self.setevent('started')
        return self._run()

    async def _run(self):
        if self.conditions:
            for event in self.conditions:
                await self.waiton(event)
        self.setevent('running')
        try:
            await self.run()
        except Exception:
            self.exception()
            self.setevent('aborted')
            raise

        else:
            self.setevent('finished')
        self.setevent('stopped')

    async def run(self):
        pass

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

    def _formatted(self, msg, *args):
        prefix = f'{self.id} ' if self.id else ''
        msg = f'{prefix}{self.type} {msg}'
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
