#!/usr/bin/env python3

import asyncio
import logging
from functools import partial

from box import Box

_l = logging.getLogger(__name__)


class Component:
    """An element in a workflow that can be configured, run, and uniquely named."""

    __instances = dict()

    def __init__(self, workflow, *args, id=None, logger=_l, **kwargs):
        self.workflow = workflow
        self.id = id
        if id:
            key = (type(self), id)
            if key in self.__instances:
                raise ValueError(f'{type(self).__name__} with ID "{id}" '
                                 f'already exists ({self.__instances[key]})')

            self.__instances[key] = self
        self.logger = logger

        self.parent = None
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
