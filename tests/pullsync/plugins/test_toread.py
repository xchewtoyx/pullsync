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
            os.path.join(TEST_DATA_DIR, 'toread.conf')
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
        self.assertIn('toread', self.app.plugin._loaded_plugins)
        self.assertIn('toread', self.app.plugin.get_enabled_plugins())

    def load_controller_test(self):
        self.app.setup()
        toread = handler.get('controller', 'toread')()
