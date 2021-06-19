#!/usr/bin/env python3

from .component import Component


class Filter(Component):

    def __call__(self, messages):
        return self.filter(messages)

    async def filter(self, messages):
        async for channel, message in messages:
            channel, message = await self.process(channel, message)
            yield (channel, message)

    async def process(self, channel, message):
        raise NotImplementedError(f'process() out {self}')


class ChannelChain(Filter):

    TESTS = dict(directory=lambda m: m.path.id_dir(),
                 file=lambda m: m.path.id_file())

    async def process(self, channel, message):
        for test_name, test_fn in self.TESTS.items():
            if test_fn(message):
                return test_name, message


class MessageMangleFilter(Filter):

    def deepget(self, mapping, key):
        if '.' not in key:
            return mapping[key]

        else:
            current, remainder = key.split('.', 1)
            return self.deepget(mapping[current], remainder)

    def deepset(self, mapping, key, value):
        if '.' not in key:
            mapping[key] = value

        else:
            current, remainder = key.split('.', 1)
            self.deepset(mapping[current], remainder, value)

    async def process(self, channel, message):
        for spec in self.settings.attributes:
            if len(spec) != 1:
                raise ValueError(f'Bad configuration: {self.settings}')

            key = list(spec.keys())[0]
            self.deepset(message, key, spec[key].format(**message))
        return channel, message
