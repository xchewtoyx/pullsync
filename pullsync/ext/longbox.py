from datetime import timedelta
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
                    sleep(2**attempt * backoff)
                else:
                    raise
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

    @with_backoff
    def check_prefix(self, gsclient, pull_id):
        file_detail = None
        prefix = 'comics/%02x/%02x/%x' % (
            pull_id & 0xff,
            (pull_id & 0xff00) >> 8,
            pull_id
        )
        file_detail = self.file_detail(pull_id)
        if not file_detail:
            request = gsclient.objects().list(bucket='long-box', prefix=prefix)
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

    def scan(self):
        unread_items = self.app.pulldb.list_unread()

        gsclient = build('storage', 'v1', http=self.app.google.client)

        cache = []
        for pull in unread_items:
            pull_detail = json.loads(self.app.redis.client.get(pull))
            pull_id = int(pull_detail['id'])
            pull_matches = self.check_prefix(gsclient, pull_id)
            if pull_matches:
                print 'File found for [%s] %s' % (
                    pull_detail['identifier'], pull_detail['name'])
                cache.append([pull, pull_matches])
                for item in pull_matches:
                    print item['mediaLink']
            else:
                print 'No match for %s' % pull_detail['identifier']
        with open('cache.json', 'w') as cache_file:
            json.dump(cache, cache_file)

class ScanController(controller.CementBaseController):
    class Meta:
        label = 'scan'
        stacked_on = 'base'
        stacked_type = 'nested'

    @controller.expose(hide=True)
    def default(self):
        self.app.longbox.scan()

def load():
    handler.register(ScanController)
    longbox = Longbox()
    hook.register('post_setup', longbox._setup)
