from datetime import timedelta
import json
import os

from cement.core import controller, handler, hook
from dateutil.parser import parse as parse_date
import xdg.BaseDirectory

from pullsync.ext import interfaces


class FetchError(Exception):
    pass


class UpdateError(Exception):
    pass


class PullDB(handler.CementBaseHandler):
    class Meta:
        interface = interfaces.DataInterface
        label = 'pulldb'

    def _setup(self, app):
        super(PullDB, self)._setup(app)
        self.app.extend('pulldb', self)
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

    def fetch_page(self, path, cursor=None, cache=True):
        if cursor:
            path = path + '?position=%s' % cursor
        self.app.log.info('Sending request for %r' % (path,))
        resp, content = self.app.google.client.request(self.base_url + path)
        if resp.status != 200:
            self.app.log.error(resp, content)
            raise FetchError('Unable to fetch unread pulls')
        else:
            with open(self.unread_file, 'w') as unread_pulls:
                unread_pulls.write(content)
            result = json.loads(content)
            if cache:
                self.app.redis.multi_set(
                    self.extract_pulls(result),
                    ttl=timedelta(1),
                )
        return result

    def fetch_new(self):
        path = '/api/pulls/list/new'
        position = None
        results = []
        while True:
            self.app.log.debug('fetching %s %r' % (path, position))
            results_page = self.fetch_page(path, cursor=position, cache=False)
            results.extend(
                [result['pull'] for result in results_page['results']])
            if not results_page['more']:
                break
            position = results_page.get('position')
        return results

    def fetch_unread(self):
        path = '/api/pulls/list/unread'
        position = None
        results = []
        while True:
            self.app.log.debug('fetching %s %r' % (path, position))
            results_page = self.fetch_page(path, cursor=position)
            results.extend(
                [result['pull'] for result in results_page['results']])
            if not results_page['more']:
                break
            position = results_page.get('position')
        return results

    def list_unread(self):
        for key in self.app.redis.client.keys('pull:*'):
            yield json.loads(self.app.redis.get(key))

    def list_unseen(self):
        for pull in self.list_unread():
            if not self.app.redis.client.sismember(
                    'gs:seen', int(pull['id'])):
                yield pull

    def pull_new(self, pull_id):
        pull_identifier = str(pull_id)
        path = '/api/pulls/update'
        data = json.dumps({
            'pull': [pull_identifier]
        })
        self.app.log.info('Sending request for: %r' % path)
        resp, content = self.app.google.client.request(
            self.base_url + path,
            method='POST',
            headers={'Content-Type': 'application/json'},
            body=data,
        )
        if resp.status != 200:
            self.app.log.error(resp, content)
            raise UpdateError('Unable to update pull %d' % pull_id)
        result = json.loads(content)['results']
        if pull_identifier in result.get('updated', []):
            self.refresh_pull(pull_id)
        else:
            self.app.log.warn('Unable to pull %d: %r' % (pull_id, result))

    def refresh_pull(self, pull_id, prefix='pull'):
        path = '/api/pulls/%d/get' % pull_id
        self.app.log.info('Sending request for: %r' % path)
        resp, content = self.app.google.client.request(self.base_url + path)
        if resp.status != 200:
            self.app.log.error(resp, content)
            raise FetchError('Unable to fetch pull %d' % pull_id)
        else:
            response = json.loads(content)
            if len(response['results']) > 1:
                self.app.log.warn(
                    'Multiple results in data store for pull id %d' % pull_id)
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


def load(app=None):
    handler.register(FetchPulls)
    pulldb = PullDB()
    hook.register('post_setup', pulldb._setup)
