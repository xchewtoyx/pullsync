import json
import os

from apiclient.http import HttpMock, HttpMockSequence
from cement.core import foundation, handler
from cement.utils import test
import mock
from mock import call

from tests.mocks import MockRedis

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def datafile(name):
    return os.path.join(TEST_DATA_DIR, name)


class TestApp(foundation.CementApp):
    class Meta:
        label = 'pullsync'
        argv = []
        config_files = [datafile('longbox.conf')]
        extensions = [
            # interfaces must go first
            'pullsync.ext.interfaces',
            'pullsync.ext.longbox',
            'pullsync.ext.pulldb',
            'pullsync.ext.rediscache',
        ]


class PulldbTest(test.CementTestCase):
    app_class = TestApp

    def ext_setup_test(self):
        self.app.setup()
        self.assertTrue(hasattr(self.app, 'longbox'))

    def check_prefix_test(self):
        self.app.setup()
        self.app.redis.client = MockRedis()
        self.app.longbox._http = HttpMockSequence([
            ({'status': 200}, open(datafile('storage.json')).read()),
            ({'status': 200}, open(datafile('storage_1002.json')).read()),
            ({'status': 200}, open(datafile('storage.json')).read()),
            ({'status': 200}, open(datafile('storage_1004.json')).read()),
        ])
        self.assertTrue(self.app.longbox.check_prefix(1002))
        self.assertFalse(self.app.longbox.check_prefix(1004))

    def scan_new_test(self):
        self.app.setup()
        self.app.redis.client = MockRedis()
        MockRedis._load_longbox_data(datafile('new_stored.json'))
        with open(datafile('new.json')) as new_file:
            new_pulls = json.load(new_file)
        self.app.pulldb.fetch_new = mock.Mock(return_value=new_pulls)
        self.app.longbox._http = HttpMockSequence([
            ({'status': 200}, open(datafile('storage.json')).read()),
            ({'status': 200}, open(datafile('storage_1000.json')).read()),
            ({'status': 200}, open(datafile('storage.json')).read()),
            ({'status': 200}, open(datafile('storage_1001.json')).read()),
            ({'status': 200}, open(datafile('storage.json')).read()),
            ({'status': 200}, open(datafile('storage_1002.json')).read()),
            ({'status': 200}, open(datafile('storage.json')).read()),
            ({'status': 200}, open(datafile('storage_1003.json')).read()),
            ({'status': 200}, open(datafile('storage.json')).read()),
            ({'status': 200}, open(datafile('storage_1004.json')).read()),
        ])
        self.app.longbox.scan(new=True)
        self.assertIn(
            call('gs:seen', 1001), self.app.redis.client.sadd.call_args_list)
        self.assertIn(
            call('gs:seen', 1002), self.app.redis.client.sadd.call_args_list)
        self.assertNotIn(
            call('gs:seen', 1003), self.app.redis.client.sadd.call_args_list)
        self.assertNotIn(
            call('gs:seen', 1004), self.app.redis.client.sadd.call_args_list)
