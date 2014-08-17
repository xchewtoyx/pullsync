import os

from cement.core import foundation, handler
from cement.utils import test

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def datafile(name):
    return os.path.join(TEST_DATA_DIR, name)


class TestApp(foundation.CementApp):
    class Meta:
        label = 'pullsync'
        argv = []
        config_files = []
        extensions = [
            # interfaces must go first
            'pullsync.ext.interfaces',
            'pullsync.ext.ext_google',
        ]


class GoogleAuthTest(test.CementTestCase):
    app_class = TestApp

    def ext_setup_test(self):
        self.app.setup()
        self.assertTrue(hasattr(self.app, 'google'))
