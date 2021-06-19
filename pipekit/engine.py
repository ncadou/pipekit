#!/usr/bin/env python3

import asyncio
import logging

from pipekit.component import ComponentInterrupted

_l = logging.getLogger(__name__)


class ETLEngine:
    """Executor for Workflow instances."""

    def __init__(self, workflow):
        self.workflow = workflow
        self.workflows = workflow.app.workflows

    def run(self):
        loop = asyncio.get_event_loop()
        _l.info(f'Workflow started ({self.workflow.source})')
        loop.run_until_complete(asyncio.gather(*list(
            n.instance.start() for w in self.workflows.values() for n in w.values())))
        _l.info(f'Workflow ended ({self.workflow.source})')
        for task in asyncio.Task.all_tasks():
            try:
                task.get_coro().throw(ComponentInterrupted)
            except RuntimeError:
                pass
        loop.close()
