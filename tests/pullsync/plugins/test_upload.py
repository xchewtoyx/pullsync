import json
import os
import sys

from cement.core import foundation, handler
from cement.utils import test
import mock

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


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
            'pullsync.ext.pulldb',
            'pullsync.ext.rediscache',
        ]
        plugin_bootstrap = 'pullsync.plugins'


class UploadPluginTest(test.CementTestCase):
    app_class = TestApp

    def load_plugin_test(self):
        self.app.setup()
        self.assertIn('upload', self.app.plugin._loaded_plugins)
        self.assertIn('upload', self.app.plugin.get_enabled_plugins())

    def normalise_name_test(self):
        self.app.setup()
        plugin = handler.get('controller', 'upload')()
        self.assertEqual(
            plugin.normalise_name('The Walking Dead #012 (2014)'),
            ('the walking dead #12', '12'),
        )

    def check_unseen_test(self):
        self.app.setup()
        plugin = handler.get('controller', 'upload')()
        plugin.app = self.app
        with open(os.path.join(TEST_DATA_DIR, 'unread.json')) as json_file:
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
        with open(os.path.join(TEST_DATA_DIR, 'new.json')) as json_file:
            unread = json.load(json_file)
        self.app.pulldb.fetch_new = mock.Mock(return_value=unread)
        results = list(plugin.identify_unseen(check_type='new'))
        self.assertEqual(len(results), 39)
