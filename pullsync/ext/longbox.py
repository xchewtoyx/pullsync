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
        label = 'scan'

    def _setup(self, app):
        self.app = app
        self.app.log.debug('Setting up longbox interface')
        self.app.longbox = self

    def file_detail(self, pull_id):
        if self.app.redis.client.sismember('gs:seen', pull_id):
            file_detail = self.app.redis.client.get('gs:file:%d' % pull_id)
            if not file_detail:
                self.app.redis.client.srem('gs:seen', pull_id)
            else:
                return json.loads(file_detail)

    @property
    def client(self):
        return build('storage', 'v1', http=self.app.google.client)

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
            request = self.client.objects().list(bucket='long-box', prefix=prefix)
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
             pipe.sadd('pulls:unread', pull.split(':')[1])
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
            pull_detail = json.loads(self.app.redis.client.get(pull))
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

    def scan(self):
        unread_items = self.app.pulldb.list_unread()

        cache = []
        for pull in unread_items:
            pull_detail = json.loads(self.app.redis.client.get(pull))
            pull_id = int(pull_detail['id'])
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
        ]

    @controller.expose(hide=True)
    def default(self):
        if self.app.pargs.full:
            self.app.longbox.scan()
        else:
            self.app.longbox.refresh()

def load():
    handler.register(ScanController)
    longbox = Longbox()
    hook.register('post_setup', longbox._setup)
