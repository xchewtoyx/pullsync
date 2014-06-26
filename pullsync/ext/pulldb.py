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
        app.log.info('Setting up pulldb handler')
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

    def fetch_unread(self):
        path = '/api/pulls/list/unread'
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

    def list_unread(self):
        return self.app.redis.client.keys('pull:*')

class FetchPulls(controller.CementBaseController):
    class Meta:
        label = 'fetch'
        stacked_on = 'base'
        stacked_type = 'nested'

    @controller.expose(hide=True)
    def default(self):
        self.app.pulldb.fetch_unread()

def load():
    handler.register(FetchPulls)
    pulldb = PullDB()
    hook.register('post_setup', pulldb._setup)
