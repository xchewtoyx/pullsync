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
            items = json.loads(self.app.redis.get('gs:file:%d' % pull_id))
            source = None
            for item in items:
                if item['contentType'] == 'application/x-cbr':
                    file_type = 'cbr'
                    source = item
                elif item['contentType'] == 'application/x-cbz':
                    file_type = 'cbz'
                    source = item
            if not source:
                raise ValueError(
                    'Cannot find file of supported type: %r' % items)
            destination = os.path.join(
                self.app.pargs.destination,
                '%06x.%s' % (pull_id, file_type)
            )
            if os.path.exists(destination):
                self.app.log.info(
                    'Skipping file %r.  Already present in destination.' % (
                        source['name']))
                continue
            self.app.longbox.fetch_file(source, destination)

        for filename in os.listdir(self.app.pargs.destination):
            if filename.endswith(('.cbr', '.cbz')):
                pull_id, file_type = filename.rsplit('.', 2)
                try:
                    pull_id = int(pull_id, 16)
                except ValueError:
                    continue
                if not self.app.redis.exists('pull:%d' % pull_id):
                    self.app.log.debug('Removing old pull %r' % filename)
                    os.unlink(os.path.join(self.app.pargs.destination, filename))

def load():
    handler.register(SyncController)
