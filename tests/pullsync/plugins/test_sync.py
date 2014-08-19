import json
import os

from apiclient.http import HttpMockSequence
from cement.core import foundation, handler
from cement.utils import test
import mock

from pullsync.plugins import sync

from tests.mocks import MockRedis

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def datafile(name):
    return os.path.join(TEST_DATA_DIR, name)


class TestApp(foundation.CementApp):
    class Meta:
        label = 'pullsync'
        argv = []
        config_files = [
            os.path.join(TEST_DATA_DIR, 'sync.conf')
        ]
        extensions = [
            # interfaces must go first
            'pullsync.ext.interfaces',
            'pullsync.ext.ext_google',
            'pullsync.ext.ext_longbox',
            'pullsync.ext.ext_pulldb',
            'pullsync.ext.ext_redis',
        ]
        plugin_bootstrap = 'pullsync.plugins'


class SyncPluginTest(test.CementTestCase):
    app_class = TestApp

    def load_plugin_test(self):
        self.app.setup()
        self.assertIn('sync', self.app.plugin._loaded_plugins)
        self.assertIn('sync', self.app.plugin.get_enabled_plugins())

    def load_controller_test(self):
        self.app.setup()
        toread = handler.get('controller', 'sync')()

    def fetch_weighted_pulls_test(self):
        self.app.setup()
        # setup httpmock
        redis_mock = MockRedis()
        redis_mock._load_pull_data(datafile('sync_unread.json'))
        self.app.redis.client = redis_mock
        sync_handler = handler.get('controller', 'sync')()
        sync_handler.app = self.app
        for pull_tuple in sync_handler.weighted_pulls():
            self.assertIsInstance(pull_tuple, tuple)

    def blah(self):
        # setup mockredis
        new_items = self.app.pulldb.list_unread()
        for pull in new_items:
            pull_detail = json.loads(self.app.redis.get(pull))
            yield float(pull_detail['weight']), pull, pull_detail
