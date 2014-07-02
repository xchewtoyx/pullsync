from datetime import timedelta
import json
import os

from cement.core import controller, handler, hook
from dateutil.parser import parse as parse_date
import xdg

class FetchError(Exception):
    pass

class PullDB(handler.CementBaseHandler):
    class Meta:
        label = 'pulldb'

    def _setup(self, app):
        app.log.debug('Setting up pulldb handler')
        self.app = app
        self.app.pulldb = self
        self.base_url = self.app.config.get('pulldb', 'base_url')
        self.data_dir = xdg.BaseDirectory.save_data_path(
            self.app._meta.label)
        self.new_file = os.path.join(self.data_dir, 'new_pulls.json')
        self.unread_file = os.path.join(self.data_dir, 'unread_pulls.json')

    def extract_pulls(self, result, prefix='pull'):
        pulls = []
        for pull in result['results']:
            key = '%s:%s' % (prefix, pull['pull']['id'])
            pulls.append((key, json.dumps(pull['pull'])))
        return pulls

    def refresh_unread(self):
        path = '/api/pulls/list/unread'
        # Set all existing keys to expire in 5 seconds
        pipe = self.app.redis.pipeline()
        for key in self.app.redis.keys('pull:*'):
            pipe.expire(key, 5)
        pipe.execute()

        position = None
        while True:
            self.app.log.debug('fetching %s %r' % (path, position))
            result = self.fetch_page(path, cursor=position)
            if not result['more']:
                break
            position = result.get('position')

    def fetch_page(self, path, cursor=None):
        if cursor:
            path = path + '?position=%s' % cursor
        resp, content = self.app.google.client.request(self.base_url + path)
        if resp.status != 200:
            self.app.log.error(resp, content)
            raise FetchError('Unable to fetch unread pulls')
        else:
            with open(self.unread_file, 'w') as unread_pulls:
                unread_pulls.write(content)
            result = json.loads(content)
            self.app.redis.multi_set(
                self.extract_pulls(result),
                ttl=timedelta(1),
            )
        return result

    def fetch_unread(self):
        path = '/api/pulls/list/unread'
        position = None
        while True:
            self.app.log.debug('fetching %s %r' % (path, position))
            result = self.fetch_page(path, cursor=position)
            if not result['more']:
                break
            position = result.get('position')

    def list_unread(self):
        return self.app.redis.client.keys('pull:*')

    def refresh_pull(self, pull_id, prefix='pull'):
        path = '/api/pulls/%d/get' % pull_id
        resp, content = self.app.google.client.request(self.base_url + path)
        if resp.status != 200:
            self.app.log.error(resp, content)
            raise FetchError('Unable to fetch pull %d' % pull_id)
        else:
            response = json.loads(content)
            for pull in response['results']:
                key = '%s:%s' % (prefix, pull['pull']['id'])
                if pull['pull']['read'] == 'True':
                    # Only cache read pulls for 30s
                    ttl = 30
                else:
                    ttl = timedelta(1)
                self.app.redis.setex(
                    key, ttl, json.dumps(pull['pull']))
                return pull['pull']

class FetchPulls(controller.CementBaseController):
    class Meta:
        label = 'fetch'
        stacked_on = 'base'
        stacked_type = 'nested'

    @controller.expose(hide=True)
    def default(self):
        self.app.pulldb.refresh_unread()

def load():
    handler.register(FetchPulls)
    pulldb = PullDB()
    hook.register('post_setup', pulldb._setup)
