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

    def weighted_pulls(self):
        new_items = self.app.pulldb.list_unread()
        for pull in new_items:
            pull_detail = json.loads(self.app.redis.get(pull))
            yield float(pull_detail['weight']), pull, pull_detail

    @controller.expose(hide=True)
    def default(self):
        new_items = sorted(self.weighted_pulls())
        for weight, pull_key, pull in new_items:
            pull_id = int(pull['identifier'])
            if not self.app.redis.sismember('gs:seen', pull_id):
                note='*'
            else:
                note=' '
            print '%06.0f%s %05x [%8.8s] %s %s' % (
                float(pull['weight']) * 1e6,
                note,
                pull_id,
                pull.get('stream_id'),
                pull['pubdate'],
                pull['name'],
            )

def load():
    handler.register(ToRead)
