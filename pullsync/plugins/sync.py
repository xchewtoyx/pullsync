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
            (['--strict'], {
                'help': (
                    'Remove any files not in the sync set, even if '
                    'unread. This will reduce the storage used in the '
                    'sync destination at the expense of additional '
                    'network traffic.'
                ),
                'action': 'store_true',
            }),
        ]

    def weighted_pulls(self):
        new_items = self.app.pulldb.list_unread()
        for pull in new_items:
            yield float(pull['weight']), pull['id'], pull

    def exportable_items(self):
        count = 0
        stalled_streams = set()
        for weight, pull, pull_detail in sorted(self.weighted_pulls()):
            if count >= self.app.pargs.count:
                break
            pull_id = int(pull_detail['identifier'])
            pull_detail = self.app.pulldb.refresh_pull(pull_id)
            self.app.log.debug('Checking pull %d' % pull_id)
            if pull_detail['read'] == 'True':
                self.app.log.debug(
                    'Issue %d is no longer unread, skipping' % pull_id
                )
                continue
            if not pull_detail.get('stream_id'):
                self.app.log.info(
                    'Skipping pull %s(%s), pull has no stream.' % (
                        pull_detail['name'],
                        pull_detail['identifier'],))
                continue
            if pull_detail['stream_id'] in stalled_streams:
                self.app.log.warn(
                    'Skipping pull %s(%s), stream %s stalled' % (
                        pull_detail['name'],
                        pull_detail['identifier'],
                        pull_detail['stream_id']))
                continue
            if not self.app.longbox.check_prefix(pull_id):
                self.app.log.warn(
                    'Issue %s(%s) not in longbox.  Stalling stream %s.' % (
                        pull_detail['name'],
                        pull_detail['identifier'],
                        pull_detail['stream_id']))
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
            match = re.search(r'\b([a-z0-9]{6})\b', path)
            if match:
                file_id = match.group(1)
                try:
                    if int(file_id, 16):
                        yield file_id
                except ValueError:
                    pass

    def safe_name(self, name):
        name = re.sub(r'/', '-', name)
        return name

    @controller.expose(hide=True)
    def default(self):
        existing_pulls = set(
            list(self.identify_pulls(self.app.pargs.destination))
        )
        self.app.log.debug('existing issues: %r' % existing_pulls)
        sync_pulls = set()
        for pull in self.exportable_items():
            pull_id = int(pull['identifier'])
            print '%06d %s' % (
                int(float(pull['weight'])*1e6),
                pull['name'],
            )
            items = self.app.longbox.check_prefix(pull_id)
            if not items:
                self.app.log.warn(
                    'Could not find file details for %d, skipping' % pull_id)
                continue
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
            hex_id = '%06x' % pull_id
            sync_pulls.add(hex_id)
            destination_name = self.safe_name(pull['name'])
            destination = os.path.join(
                self.app.pargs.destination,
                '%s [%s].%s' % (destination_name, hex_id, file_type)
            )
            if hex_id in existing_pulls:
                self.app.log.info(
                    'Skipping file %r.  Already present in destination.' % (
                        source['name']))
                continue
            self.app.log.info('Fetching %r -> %r' % (
                source['name'], destination))
            self.app.longbox.fetch_file(source, destination)

        if self.app.pargs.strict:
            expired_pulls = existing_pulls - sync_pulls
        else:
            expired_pulls = []
            for hex_id in existing_pulls:
                pull_key = 'pull:%d' % int(hex_id, 16)
                if not self.app.redis.get(pull_key):
                    expired_pulls.append(hex_id)
        self.app.log.debug('%r - %r = %r' % (
            existing_pulls, sync_pulls, expired_pulls)
        )

        if expired_pulls:
            self.expire_pulls(self.app.pargs.destination, expired_pulls)


def load(app=None):
    handler.register(SyncController)
