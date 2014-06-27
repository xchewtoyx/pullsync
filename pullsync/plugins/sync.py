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

    def expire_pulls(self, directory, expired_pulls):
        for filename in os.listdir(self.app.pargs.destination):
            if filename.endswith(('.cbr', '.cbz')):
                for expired_id in expired_pulls:
                    if expired_id in filename:
                        self.app.log.debug('Removing old pull %r' % filename)
                        os.unlink(os.path.join(
                            self.app.pargs.destination, filename))

    def identify_pulls(self, directory):
        for path in os.listdir(directory):
            match = re.search(r'\b([a-z0-9]{6})\b')
            if match:
                file_id = match.group(1)
                try:
                    if int(file_id, 16):
                        yield match
                except ValueError:
                    pass

    @controller.expose(hide=True)
    def default(self):
        existing_pulls = set(self.identify_pulls(self.app.pargs.destination))
        sync_pulls = set()
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
            sync_pulls.add(pull['identifier'])
            destination = os.path.join(
                self.app.pargs.destination,
                '%s[%06x].%s' % (pull['name'], pull_id, file_type)
            )
            if pull['identifier'] in existing_pulls:
                self.app.log.info(
                    'Skipping file %r.  Already present in destination.' % (
                        source['name']))
                continue
            self.app.longbox.fetch_file(source, destination)

        expired_pulls = existing_pulls - sync_pulls

        if expired_pulls:
            self.expire_pulls(self.app.pargs.destination, expired_pulls)

def load():
    handler.register(SyncController)
