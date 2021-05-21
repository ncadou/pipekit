#!/usr/bin/env python3

import logging
import os
import sys
from importlib import import_module
from inspect import isclass
from pathlib import Path
from string import Template

import strictyaml
from box import Box

from .engine import ETLEngine
from .node import Node, PriorityRegistry
from .pipe import PipeRef
from .utils import isdict, islist, isstr

_l = logging.getLogger(__name__)


class Workflow:
    def __init__(self, source, settings=None):
        self.settings = Box(settings or {})
        self.read(source)
        self.app = None

    def read(self, source):
        """Load workflow from definition source (file path or string)."""
        self.source = source
        if source == '-':
            source = sys.stdin.read()
        else:
            try:
                params = os.environ.copy()
                params.update(self.settings.params)
                source = Template(Path(source).read_text()).substitute(params)
            except FileNotFoundError:
                self.source = None
            except KeyError as e:
                raise ConfigurationError(
                    f'Missing environment variable "{e.args[0]}" (needed by {source})')

        self.definition = self.expand(Box(strictyaml.load(source).data, box_dots=True))

    __MISSING__ = object()

    def expand(self, settings, element=__MISSING__, seen=None):
        """Recursively expand all "<use>" references."""
        if element is self.__MISSING__:
            element = settings
        if seen is None:
            seen = set()
        if islist(element):
            return type(element)(self.expand(settings, e, seen=seen) for e in element)

        elif isdict(element):
            useref = element.pop('<use>', None)
            if useref:
                seen.add(useref)
                try:
                    useref = type(element)(settings[useref].to_dict())
                except KeyError:
                    raise ConfigurationError(f'Reference "{useref}" not found')

                useref.merge_update(element)
                element = useref
            return type(element)((k, self.expand(settings, e, seen=seen))
                                 for k, e in element.items())

        return element

    def load(self):
        _l.info(f'Loading workflow from {self.source}')
        if self.app:
            raise RuntimeError('Workflow has already been loaded')

        self.app = Box(self.definition.to_dict(), box_dots=True)
        self.configure()
        self.build()

    def configure(self):
        """Locate and configure components."""
        _l.debug('Configuring workflow')
        for wname, workflow in self.app.workflows.items():
            for nname, node in workflow.items():
                node.workflow = wname
                node.key = f'{wname}.{nname}'
                self._configure_node(node)
                node.setdefault('outbox', dict(default=None))
                for msgbox in ('inbox', 'outbox'):
                    self._configure_pipes(node, msgbox)
        for wname, workflow in self.app.workflows.items():
            for node in workflow.values():
                for msgbox in ('inbox', 'outbox'):
                    self._configure_connections(node, msgbox)

    def _configure_node(self, node):
        try:
            node.component = resolve(node.component)
        except Exception:
            errmsg = f'Failed to import node "{node.get("component")}" (needed by {node.key})'
            _l.exception(errmsg)
            raise ConfigurationError(errmsg)
        if 'conditions' in node:
            if isinstance(node.conditions, str):
                node.conditions = [node.conditions]
            for i, condition in enumerate(node.conditions):
                if '.' not in condition:
                    node.conditions[i] = f'{node.workflow}.{condition}'

    def _configure_pipes(self, node, msgbox):
        # Get default channels from class, if any.
        defaults = getattr(node.component, msgbox.upper(), dict())
        if defaults:
            if islist(defaults):
                defaults = dict((c, None) for c in defaults)
            elif not isdict(defaults):
                raise ConfigurationError(f'Default channels for {node.component} misconfigured')

        # Expand shortened configuration structure, if needed.
        channels = node.get(msgbox, {})
        if isstr(channels) or (
                isdict(channels) and ('node' in channels or 'component' in channels)):
            channels = dict(default=channels)
        elif islist(channels):
            channels = dict(zip(channels, [None] * len(channels)))
        channels = Box(defaults, **channels)

        # Resolve pipe specs to classes.
        for name, spec in channels.items():
            channel = Box()
            if not spec:
                spec = 'pipekit.pipe:QueuePipe'
            if isstr(spec):
                if spec.startswith('<'):
                    break  # reference will be later resolved in _configure_connections()

                else:
                    channel.component = spec
            elif isdict(spec):
                channel.update(spec)
            else:
                raise ConfigurationError(
                    f'Incorrect {msgbox} configuration for node {node.key}: unknown spec {spec!r}')

            if 'component' in channel:
                channel.component = resolve(channel.component)
            channels[name] = channel

        node[msgbox] = channels

    def _configure_connections(self, node, msgbox):
        for name, channel in node[msgbox].items():
            if isstr(channel):
                if not channel.startswith('<'):
                    raise ConfigurationError('Unrecognized configuration item for '
                                             f'{node.key}.{msgbox}:{name}: {channel}')

                spec = channel.strip('< ')
                peer_node, peer_channel = self.peer_node(spec, node)
                peer_msgbox = 'outbox' if msgbox == 'inbox' else 'inbox'
                node[msgbox][name] = PipeRef(peer_node, peer_msgbox, peer_channel)

    def build(self):
        """Instantiate and wire up nodes and pipes."""
        _l.debug('Instantiating workflow')
        self._node_backlog = dict()
        for workflow in self.app.workflows.values():
            for _, node in workflow.items():
                self._node_backlog[node.key] = node
        for node in list(self._node_backlog.values()):
            try:
                _l.info(f'Creating node {node.key}')
                self.make_node(node)
            except Exception:
                _l.exception(f'Error while instantiating node {node.key}')
                raise

    RESERVED_SETTINGS = set(['id', 'process', 'scale', 'inbox', 'ifilters', 'ofilters', 'outbox'])

    def make_node(self, node):
        """Instantiate a node and all its upstream nodes, and wire them together."""
        if node.key not in self._node_backlog:
            return

        settings = node.get('settings', {})
        reserved_settings = self.RESERVED_SETTINGS.intersection(set(settings.keys()))
        if reserved_settings:
            plural = 's' if len(reserved_settings) > 1 else ''
            raise ConfigurationError(f'Settings for node {node.key} contains reserved '
                                     f'key{plural}: {", ".join(reserved_settings)}')

        outbox = dict()
        for channel, pipe in node.outbox.items():
            node.outbox[channel].instance = self.make_component(
                pipe.component, id=f'{node.key}.output.{channel}', **pipe.get('settings', {}))
            outbox[channel] = node.outbox[channel].instance

        inbox = dict()
        for channel, pipe in node.inbox.items():
            if hasattr(pipe, 'instance'):
                inbox[channel] = pipe.instance
                raise Exception('node instance exists')

            if isinstance(pipe, PipeRef):
                try:
                    node.inbox[channel] = pipe = Box(component=pipe, instance=pipe.resolve())
                except KeyError:
                    raise ConfigurationError(f'Pipe not found: {pipe}')

            else:
                pipe.instance = self.make_component(
                    pipe.component, id=f'{node.key}.input.{channel}', **pipe.get('settings', {}))
            inbox[channel] = pipe.instance

        ifilters = self._make_filters(node, 'ifilters')
        ofilters = self._make_filters(node, 'ofilters')
        node_args = dict(
            id=node.key, blocking=bool(node.get('blocking')), scale=node.get('scale'), inbox=inbox,
            ifilters=ifilters, ofilters=ofilters, outbox=outbox,
            conditions=node.get('conditions', []), **settings)
        if isclass(node.component) and issubclass(node.component, Node):
            node_class = node.component
        elif callable(node.component):
            node_class = Node
            node_args['process'] = node.component
        else:
            raise ConfigurationError(
                f'Node {node.key} should be a subclass of Node or a callable, '
                f'got {type(node.component)} instead')

        node.instance = self.make_component(node_class, **node_args)
        del self._node_backlog[node.key]

    def _make_filters(self, node, type_):
        """Instantiate and return filters wrapped in a PriorityRegistry."""
        filters = node.get(type_, {})
        for name, filter_ in filters.items():
            filter_.instance = self.make_component(
                resolve(filter_.component), id=f'{node.key}.{type_}.{name}',
                **filter_.get('settings', {}))
        return PriorityRegistry(dict((k, f.instance) for k, f in filters.items()))

    def peer_node(self, spec, dependent):
        """Return node instance and channel referenced in spec."""
        spec, channel, *_ = spec.rsplit(':', 1) + ['default']
        spec = spec.split('.')
        node = spec[-1]
        node_key = '.'.join(spec[:-1] + [node, ])
        try:
            node = self.app.workflows[node_key]
        except KeyError:
            try:
                node = self.app.workflows[dependent.workflow][node_key]
            except KeyError:
                raise ConfigurationError(
                    f'Undefined node "{node_key}" (needed by {dependent.key})')
        return node, channel

    def make_component(self, class_, *args, **kwargs):
        return class_(self, *args, **kwargs)

    _SECRETS = set('account password secret'.split())

    def safe_settings(self, settings=__MISSING__):
        """Return modified settings where secrets have been hidden."""
        if settings is self.__MISSING__:
            settings = self.app
        if isdict(settings):
            settings = settings.copy()
            for key, value in settings.items():
                if isdict(value):
                    settings[key] = self.safe_settings(value)
                elif islist(value):
                    settings[key] = [self.safe_settings(i) for i in value]
                elif key in self._SECRETS:
                    settings[key] = '<secret>'
        return settings

    def run(self):
        """Create engine and run workflow."""
        self.engine = ETLEngine(self)
        self.engine.run()


def resolve(spec):
    """Parse component textual spec and import and return corresponding object."""
    try:
        module, attr, *_ = spec.rsplit(':', 1) + [None]
        module = import_module(module)
        return getattr(module, attr)

    except Exception:
        raise ImportError(f'Failed to import component from spec: {spec}')


class ConfigurationError(Exception):
    pass
