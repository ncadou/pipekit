#!/usr/bin/env python3

import asyncio
import logging
from collections import defaultdict
from functools import partial

from box import Box

_l = logging.getLogger(__name__)

_instances = dict()

_events = defaultdict(asyncio.Event)
_event_queues = list()
_event_callbacks = defaultdict(list)


class Component:
    """A stateful element in a workflow that can be configured, run, and uniquely named."""

    def __init__(self, *args, id=None, workflow=None, parent=None, logger=_l, **kwargs):
        self.id = id
        if id:
            key = (type(self), id)
            if key in _instances:
                raise ValueError(
                        f'{key[0].__name__} with ID "{id}" already exists: {_instances[key]}')

            _instances[key] = self
        self.workflow = workflow
        self.parent = parent
        self.children = list()
        if parent:
            parent.children.append(self)
        self.logger = logger

        self.loop = asyncio.get_event_loop()

        self._event_lock = set()
        self._debug = {'events'}

        self._settings = Box(self.configure(*args, **kwargs) or dict())
        if not workflow:
            workflow = self
        settings = [f'{k}={v}' for k, v in workflow.safe_settings(self._settings).items()]
        self.debug(f'Initialized {" ".join(settings)}')

    def configure(self, **settings):
        return settings

    def settings(self, **override):
        return Box(self._settings, **override)

    @property
    def type(self):
        return type(self).__name__

    @property
    def status(self):
        return getattr(self, '_status', None)

    @status.setter
    def status(self, status):
        if not (self.hasstatus(status) or status in self._event_lock):
            self._event_lock.add(status)
            try:
                self._status_setter(status)
            finally:
                self._event_lock.remove(status)

    _dependent_statuses = {'processing-finished', 'finished', 'exited'}

    def _status_setter(self, status):
        event = status if isinstance(status, ComponentEvent) else ComponentEvent(status, self)
        if event.status in self._dependent_statuses:
            children = set(filter(lambda c: isinstance(c, Component), self.children))
            ready = set(filter(lambda c: c.hasstatus(event.status), children))
            if len(children) > len(ready):
                if 'events' in self._debug:
                    pending = ", ".join(c.id for c in children.difference(ready))
                    self.debug(f'Status "{event.status}" waiting on {pending}')
                return

        if self.hasstatus('aborted') and event.status != 'exited':
            if 'events' in self._debug:
                self.debug(f'Ignoring status "{event.status}" because the component is '
                           'in aborted state')
            return

        # event.id = self._fqevent(status)
        if 'events' in self._debug:
            self.debug(f'Emitting event "{event.id}"')
        self._status = event.status
        _events[event.id].set()
        for queue in _event_queues:
            queue.put_nowait(event)
        if self.parent and event.status != 'aborted' and not isinstance(self, LocalEvents):
            self.parent.status = event.status
        for callback in _event_callbacks[event.id]:
            asyncio.ensure_future(callback())
        _event_callbacks[event.id].clear()

    def hasstatus(self, status):
        """Return `True` if given status was set."""
        if isinstance(status, ComponentEvent):
            event = status.id
        elif ':' in status:
            event = status
        else:
            event = ComponentEvent(status, self).id
        return _events[event].is_set()

    async def waiton(self, event):
        if 'events' in self._debug:
            self.debug(f'Waiting on event "{event}"')
        await _events[event].wait()
        if 'events' in self._debug:
            self.debug(f'Received event "{event}"')

    @property
    def running(self):
        """Return `True` if in one of the running states."""
        if not self.stopped:
            for status in ['started', 'running']:
                if self.hasstatus(status):
                    return True

    @property
    def stopped(self):
        """Return `True` if in one of the stopped states."""
        for status in ['aborted', 'finished']:
            if self.hasstatus(status):
                return True

    @property
    def aborted(self):
        """Return `True` if the aborted event was emitted."""
        return self.hasstatus('aborted')

    def start(self):
        self.status = 'started'
        return self.run()

    def stop(self):
        self.debug('Stopping')

    def abort(self, exception=None):
        if self.hasstatus('aborted'):
            return

        self.status = ComponentEvent('aborted', self, exception)
        for child in self.children:
            if child.settings().get('error-propagation') in ('none', 'up'):
                if 'events' in self._debug:
                    self.debug(f'Suppressing error propagation to child {child.id}')
            elif not child.hasstatus('aborted'):
                if 'events' in self._debug:
                    self.debug(f'Propagating error to child {child.id}')
                child.abort()
        if self.parent:
            if self.parent.settings().get('error-propagation') in ('none', 'down'):
                if 'events' in self._debug:
                    self.debug(f'Suppressing error propagation to parent {self.parent.id}')
            elif not self.parent.hasstatus('aborted'):
                if 'events' in self._debug:
                    self.debug(f'Propagating error to parent {self.parent.id}')
                self.parent.abort(exception)

    def __getattr__(self, name):
        if name not in ('critical', 'error', 'warning', 'info', 'debug', 'exception'):
            raise AttributeError(f"'{self.type}' object has no attribute '{name}'")

        return partial(self._proxied_logging_method, name)

    def _proxied_logging_method(self, method, *args, **kwargs):
        if method == 'debug':
            debug = (self.workflow or self).settings().logging.debug
            if not ('all' in debug or self.type in debug or (self.id in debug)):
                return lambda *a, **kw: None

        return getattr(self.logger, method)(*self._log_formatted(*args), **kwargs)

    def _log_formatted(self, msg, *args):
        """Return the msg prefixed with this component's ID and type."""
        prefix = f'{self.id} ' if self.id else ''
        msg = f'{prefix}({self.type}) {msg}'
        return (msg,) + args

    async def run(self):
        self.status = 'running'

    async def try_while_running(self, callable, timeout=0.5):
        """Return result of `callable`, or raise `ComponentInterrupted` if component is stopped."""
        while self.running:
            coro = callable()
            try:
                return await asyncio.wait_for(coro, timeout)

            except asyncio.TimeoutError:
                pass

        raise ComponentInterrupted


class ComponentEvent:
    def __init__(self, status, component, exception=None):
        self.status = status
        self.component = component
        self.exception = exception

    @property
    def id(self):
        """Return a fully qualified ID string representing this event."""
        return f'{self.component.id}:{self.status}'


class LocalEvents:
    pass


class ComponentInterrupted(Exception):
    pass


def get_event_listener():
    """Return a new `Queue` object that will see all events."""
    queue = asyncio.Queue()
    _event_queues.append(queue)
    return queue


def add_event_callback(event, callable, *args, **kwargs):
    """Register a callback that will be called upon the given event."""
    _event_callbacks[event].append(partial(callable, *args, **kwargs))
