from datetime import timedelta
import io
import json
import os
import re
import time

import apiclient
from apiclient.discovery import build
from cement.core import controller, handler, hook, interface
from dateutil.parser import parse as parse_date
from Levenshtein import distance

from pullsync.ext import interfaces


def with_backoff(original_function):
    def retry_with_backoff(*args, **kwargs):
        backoff = 0.1
        attempt = 0
        while True:
            try:
                return original_function(*args, **kwargs)
            except apiclient.errors.HttpError:
                if attempt < 5:
                    time.sleep(2**attempt * backoff)
                else:
                    raise
            finally:
                attempt += 1
    return retry_with_backoff


class Longbox(controller.CementBaseController):
    class Meta:
        interface = interfaces.DataInterface
        label = 'longbox'

    def _setup(self, app):
        super(Longbox, self)._setup(app)
        self.app.log.debug('Setting up longbox interface')
        self.app.extend('longbox', self)
        # _http can be used to insert a HTTP Mock for testing
        self._http = None

    def file_detail(self, pull_id):
        if self.app.redis.client.sismember('gs:seen', pull_id):
            file_detail = self.app.redis.client.get('gs:file:%d' % pull_id)
            if not file_detail:
                self.app.redis.client.srem('gs:seen', pull_id)
            else:
                return json.loads(file_detail)

    @property
    def client(self):
        if not self._http:
            self._http = self.app.google.client
        return build('storage', 'v1', http=self._http)

    @with_backoff
    def check_prefix(self, pull_id):
        file_detail = None
        prefix = 'comics/%02x/%02x/%x' % (
            pull_id & 0xff,
            (pull_id & 0xff00) >> 8,
            pull_id
        )
        file_detail = self.file_detail(pull_id)
        if not file_detail:
            request = self.client.objects().list(
                bucket='long-box', prefix=prefix)
            response = request.execute()
            if 'items' in response:
                self.app.log.debug('Files found for prefix %s' % prefix)
                file_detail = response['items']
                self.app.redis.client.sadd('gs:seen', pull_id)
                self.app.redis.client.setex(
                    'gs:file:%d' % pull_id,
                    timedelta(7),
                    json.dumps(file_detail),
                )
        return file_detail

    def fetch_file(self, item_detail, destination):
        # Get Payload Data
        req = self.client.objects().get_media(
            bucket=item_detail['bucket'],
            object=item_detail['name'],
        )

        # The BytesIO object may be replaced with any io.Base instance.
        fh = io.FileIO(destination, 'w')
        downloader = apiclient.http.MediaIoBaseDownload(
            fh, req, chunksize=1024*1024)
        done = False
        try:
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    self.app.log.debug(
                        'Download %02d%%.' % int(status.progress() * 100)
                    )
        except apiclient.errors.HttpError as error:
            self.app.log.error('Error downloading file: %r' % error)
            os.unlink(destination)

    def refresh(self):
        self.app.redis.delete('pulls:seen')
        pipe = self.app.redis.pipeline()
        for pull in self.app.pulldb.list_unread():
            pipe.sadd('pulls:unread', pull['identifier'])
        pipe.execute()
        unseen_items = self.app.redis.sdiff('pulls:unread', 'gs:seen')
        self.app.log.debug('%r - %r = %r' % (
            self.app.redis.smembers('pulls:unread'),
            self.app.redis.smembers('gs:seen'),
            unseen_items))

        cache = []

        for pull_id in unseen_items:
            pull = 'pull:%s' % pull_id
            pull_id = int(pull_id)
            pull_detail = self.app.redis.client.get(pull)
            if pull_detail:
                pull_detail = json.loads(pull_detail)
            else:
                print 'skipping %d' % pull_id
                continue
            pull_matches = self.check_prefix(pull_id)
            if pull_matches:
                self.app.log.debug('File found for [%s] %s' % (
                    pull_detail['identifier'], pull_detail['name']))
                cache.append([pull, pull_matches])
                for item in pull_matches:
                    print item['name']
            else:
                self.app.log.debug('No match for %s' % (
                    pull_detail['identifier'],)
                )

    def scan(self, new=False):
        if new:
            unread_items = self.app.pulldb.fetch_new()
        else:
            unread_items = self.app.pulldb.list_unread()

        for pull_detail in unread_items:
            pull_id = int(pull_detail['id'])
            pull_matches = self.check_prefix(pull_id)
            if pull_matches:
                self.app.log.debug('File found for [%s] %s' % (
                    pull_detail['identifier'], pull_detail['name']))
                for item in pull_matches:
                    print item['name']
            else:
                self.app.log.debug('No match for %s' % (
                    pull_detail['identifier'],)
                )


class ScanController(controller.CementBaseController):
    class Meta:
        label = 'scan'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['--full'], {
                'action': 'store_true',
                'help': 'Scan all issues',
            }),
            (['--new', '-n'], {
                'action': 'store_true',
                'help': 'Scan only new issues',
            }),
        ]

    @controller.expose(hide=True)
    def default(self):
        if self.app.pargs.full or self.app.pargs.new:
            self.app.longbox.scan(new=self.app.pargs.new)


def load():
    handler.register(ScanController)
    longbox = Longbox()
    hook.register('post_setup', longbox._setup)
