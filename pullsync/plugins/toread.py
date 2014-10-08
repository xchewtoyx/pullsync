import json
import os
import re
import time

from cement.core import controller, handler
from dateutil.parser import parse as parse_date


class ToRead(controller.CementBaseController):
    class Meta:
        label = 'toread'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['--new', '-n'], {
                'help': 'Check against the pulldb api for new issues',
                'action': 'store_true',
            }),
        ]

    def weighted_pulls(self, new=False):
        if new:
            new_items = self.app.pulldb.fetch_new()
        else:
            new_items = self.app.pulldb.list_unread()
        for pull_detail in new_items:
            pull = 'pull:%s' % pull_detail['identifier']
            yield float(pull_detail['weight']), pull, pull_detail

    @controller.expose(hide=True)
    def default(self):
        new_items = sorted(self.weighted_pulls(new=self.app.pargs.new))
        try:
            for weight, pull_key, pull in new_items:
                pull_id = int(pull['identifier'])
                if not self.app.redis.sismember('gs:seen', pull_id):
                    note = '*'
                else:
                    note = ' '
                print '%07.0f%s %6d [%8.8s] %s %s' % (
                    float(pull['weight']) * 1e6,
                    note,
                    pull_id,
                    pull.get('stream_id'),
                    pull['pubdate'],
                    pull['name'],
                )
        except IOError:
            # Suppress Broken pipe error
            pass


def load(app=None):
    handler.register(ToRead)
