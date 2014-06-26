import json
import os
import re
import time

from cement.core import controller, handler
from dateutil.parser import parse as parse_date

class SyncController(controller.CementBaseController):
    class Meta:
        label = 'sync'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['--count'], {
                'help': 'Number of items to sync',
                'type': int,
                'default': 25,
            }),
            (['--destination'], {
                'help': 'Directory to sync files to',
                'action': 'store',
                'required': True,
            }),
        ]

    def weighted_pulls(self):
        new_items = self.app.pulldb.list_unread()
        for pull in new_items:
            pull_detail = json.loads(self.app.redis.get(pull))
            yield float(pull_detail['weight']), pull, pull_detail

    def exportable_items(self):
        count = 0
        stalled_streams = set()
        for weight, pull, pull_detail in sorted(self.weighted_pulls()):
            if count >= self.app.pargs.count:
                break
            pull_id = int(pull_detail['identifier'])
            pull_detail = self.app.pulldb.refresh_pull(pull_id)
            if pull_detail['read'] == 'True':
                self.app.log.debug(
                    'Issue %d is no longer unread, skipping' % pull_id
                )
                continue
            if not pull_detail.get('stream_id'):
                self.app.log.info(
                    'Skipping pull %s(%s), pull has no stream.' % (
                        pull_detail['name'],
                        pull_detail['identifier'],
                ))
                continue
            if pull_detail['stream_id'] in stalled_streams:
                self.app.log.warn(
                    'Skipping pull %s(%s), stream %s stalled' % (
                        pull_detail['name'],
                        pull_detail['identifier'],
                        pull_detail['stream_id']
                ))
                continue
            if not self.app.redis.sismember('gs:seen', pull_id):
                self.app.log.warn(
                    'Issue %s(%s) not in longbox.  Stalling stream %s.' % (
                        pull_detail['name'],
                        pull_detail['identifier'],
                        pull_detail['stream_id']
                ))
                stalled_streams.add(pull_detail['stream_id'])
                continue
            yield pull_detail
            count = count + 1

    @controller.expose(hide=True)
    def default(self):
        for pull in self.exportable_items():
            pull_id = int(pull['identifier'])
            print '%06d %s' % (
                int(float(pull['weight'])*1e6),
                pull['name'],
            )

def load():
    handler.register(SyncController)
