import json
import os
import sys

from apiclient.http import HttpMockSequence
from cement.core import foundation, handler
from cement.utils import test
import mock

from pullsync.plugins import upload

from tests.mocks import MockRedis

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def datafile(name):
    return os.path.join(TEST_DATA_DIR, name)


class ProcessError(Exception):
    pass


class TestApp(foundation.CementApp):
    class Meta:
        label = 'pullsync'
        argv = []
        config_files = [
            os.path.join(TEST_DATA_DIR, 'upload.conf')
        ]
        extensions = [
            # interfaces must go first
            'pullsync.ext.interfaces',
            'pullsync.ext.ext_google',
            'pullsync.ext.ext_longbox',
            'pullsync.ext.ext_matcher',
            'pullsync.ext.ext_pulldb',
            'pullsync.ext.ext_redis',
        ]
        plugin_bootstrap = 'pullsync.plugins'


class UploadPluginTest(test.CementTestCase):
    app_class = TestApp

    def load_plugin_test(self):
        self.app.setup()
        self.assertIn('upload', self.app.plugin._loaded_plugins)
        self.assertIn('upload', self.app.plugin.get_enabled_plugins())

    def check_unseen_test(self):
        self.app.setup()
        plugin = handler.get('controller', 'upload')()
        plugin.app = self.app
        with open(datafile('unread.json')) as json_file:
            unread = json.load(json_file)
        test_keys = ["pull:%s" % p["identifier"] for p in unread]
        test_dict = {
            key: json.dumps(value) for key, value in zip(test_keys, unread)
        }
        client = mock.Mock()
        client.keys = mock.Mock(return_value=test_keys)
        client.get = mock.Mock(
            side_effect=lambda k: test_dict[k]
        )
        self.app.redis.client = client
        self.app.pulldb.fetch_unread = mock.Mock(return_value=unread)
        client.sismember = mock.Mock(return_value=True)
        results = list(plugin.identify_unseen(check_type='unseen'))
        self.assertEqual(len(results), 0)
        client.sismember = mock.Mock(return_value=False)
        results = list(plugin.identify_unseen(check_type='unseen'))
        self.assertEqual(len(results), len(test_keys))

    def check_new_test(self):
        self.app.setup()
        plugin = handler.get('controller', 'upload')()
        plugin.app = self.app
        self.app.google._http = HttpMockSequence([
            ({'status': 200}, open(datafile('fetch_new.json')).read()),
        ])
        results = list(plugin.identify_unseen(check_type='new'))
        self.assertEqual(len(results), 7)

    def commit_file_test(self):
        self.app.setup()
        plugin = handler.get('controller', 'upload')()
        plugin.app = self.app
        self.app.redis.client = MockRedis()
        self.app.google._http = HttpMockSequence([
            ({'status': 200}, open(datafile(
                'pull_update_pull_1000.json')).read()),
            ({'status': 200}, open(
                datafile('pull_fetch_1000.json')).read()),
        ])
        self.app.longbox._http = HttpMockSequence([
            ({'status': 200}, open(datafile('storage.json')).read()),
            ({'status': 200}, open(datafile('storage_1000.json')).read()),
        ])
        candidate = ('.', 'Test Issue 1 (2014).cbr')
        with open(datafile('pull_data_1000.json')) as pull_data:
            best_match = json.load(pull_data)
        # cases: unread pull, file exists
        upload.subprocess.check_call = mock.Mock()
        plugin.commit_file(best_match, candidate)
        self.app.redis.client.sadd.assert_called_with('gs:seen', 1000)

    def commit_file_nomatch_fail_test(self):
        self.app.setup()
        plugin = handler.get('controller', 'upload')()
        plugin.app = self.app
        self.app.redis.client = MockRedis()
        self.app.longbox._http = HttpMockSequence([
            ({'status': 200}, open(datafile('storage.json')).read()),
            ({'status': 200}, open(datafile('storage_nomatch.json')).read()),
        ])
        candidate = ('.', 'Test Issue 1 (2014).cbr')
        with open(datafile('pull_data_1000.json')) as pull_data:
            best_match = json.load(pull_data)
        # cases: unread pull, no file, transfer fails
        upload.subprocess.check_call = mock.Mock(side_effect=ProcessError)
        self.app.redis.client.sadd.reset_mock()
        self.app.redis.client.set.reset_mock()
        with self.assertRaises(ProcessError):
            plugin.commit_file(best_match, candidate)
        assert not self.app.redis.client.sadd.called, (
            'redis.client.sadd called unexpectedly')
        assert not self.app.redis.client.set.called, (
            'redis.client.set called unexpectedly')

    def commit_file_nomatch_test(self):
        self.app.setup()
        plugin = handler.get('controller', 'upload')()
        plugin.app = self.app
        self.app.redis.client = MockRedis()
        self.app.google._http = HttpMockSequence([
            ({'status': 200}, open(datafile(
                'pull_update_pull_1000.json')).read()),
            ({'status': 200}, open(
                datafile('pull_fetch_1000.json')).read()),
        ])
        self.app.longbox._http = HttpMockSequence([
            ({'status': 200}, open(datafile('storage.json')).read()),
            ({'status': 200}, open(datafile('storage_nomatch.json')).read()),
            ({'status': 200}, open(datafile('storage.json')).read()),
            ({'status': 200}, open(datafile('storage_1000.json')).read()),
        ])
        candidate = ('.', 'Test Issue 1 (2014).cbr')
        with open(datafile('pull_data_1000.json')) as pull_data:
            best_match = json.load(pull_data)

        # cases: unread pull, no file, transfer good
        upload.subprocess.check_call = mock.Mock()
        plugin.commit_file(best_match, candidate)

        self.app.redis.client.sadd.assert_called_with('gs:seen', 1000)
        self.assertNotIn('pull:1000', self.app.redis.client.additions)

    def commit_new_test(self):
        self.app.setup()
        plugin = handler.get('controller', 'upload')()
        plugin.app = self.app
        self.app.redis.client = MockRedis()
        self.app.google._http = HttpMockSequence([
            ({'status': 200}, open(datafile('storage.json')).read()),
            ({'status': 200}, open(datafile('storage_1001.json')).read()),
            ({'status': 200}, open(
                datafile('pull_update_pull_1001.json')).read()),
            ({'status': 200}, open(
                datafile('pull_fetch_1001.json')).read()),
        ])
        candidate = ('.', 'Test Issue 2 (2014).cbr')
        with open(datafile('pull_data_1001.json')) as pull_data:
            best_match = json.load(pull_data)
        # cases: new pull, file exists
        upload.subprocess.check_call = mock.Mock()
        plugin.commit_file(best_match, candidate)
        self.app.redis.client.sadd.assert_called_with('gs:seen', 1001)
        self.assertIn('pull:1001', self.app.redis.client.additions)

    def commit_new_nomatch_test(self):
        self.app.setup()
        plugin = handler.get('controller', 'upload')()
        plugin.app = self.app
        self.app.redis.client = MockRedis()
        self.app.google._http = HttpMockSequence([
            ({'status': 200}, open(
                datafile('pull_update_pull_1001.json')).read()),
            ({'status': 200}, open(
                datafile('pull_fetch_1001.json')).read()),
        ])
        self.app.longbox._http = HttpMockSequence([
            ({'status': 200}, open(datafile('storage.json')).read()),
            ({'status': 200}, open(datafile('storage_nomatch.json')).read()),
            ({'status': 200}, open(datafile('storage.json')).read()),
            ({'status': 200}, open(datafile('storage_1001.json')).read()),
        ])
        candidate = ('.', 'Test Issue 2 (2014).cbr')
        with open(datafile('pull_data_1001.json')) as pull_data:
            best_match = json.load(pull_data)
        # cases: new pull, no file
        upload.subprocess.check_call = mock.Mock()
        plugin.commit_file(best_match, candidate)

    def scan_dir_test(self):
        self.app.setup()
        plugin = handler.get('controller', 'upload')()
        plugin.app = self.app

        data_files = [
            'Test Issue 1 (2014).cbr',
            'Test Issue 2 (2014).cbr',
            'Test Issue 3 (2014).cbr',
            'Test Issue 4 (2014).cbr',
            'Test Issue 5 (2014).cbr',
        ]
        for path, filename in plugin.scan_dir(datafile('scan_test')):
            self.assertIn(filename, data_files)

    def find_matches_test(self):
        self.app.setup()
        plugin = handler.get('controller', 'upload')()
        plugin.app = self.app
        candidates = [
            ('.', 'Test Issue 1 (2014).cbr'),
            ('.', 'Test Issue 2 (2014).cbr'),
            ('.', 'Test Issue 3 (2014).cbr'),
            ('.', 'Test Issue 4 (2014).cbr'),
            ('.', 'Test Issue 5 (2014).cbr'),
        ]
        results = json.load(open(datafile('fetch_new.json')))
        pulls = [entry['pull'] for entry in results['results']]
        for good_match, best_match, candidate in plugin.find_matches(
                candidates, pulls, 0.25):
            self.assertFalse(good_match)
