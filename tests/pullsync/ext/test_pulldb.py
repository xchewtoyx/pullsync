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
            os.path.join(TEST_DATA_DIR, 'pulldb.conf')
        ]
        extensions = [
            # interfaces must go first
            'pullsync.ext.interfaces',
            'pullsync.ext.pulldb',
        ]


class PulldbTest(test.CementTestCase):
    app_class = TestApp

    def fetch_unread_test(self):
        self.app.setup()
        with open(os.path.join(TEST_DATA_DIR, 'unread.json')) as json_file:
            unread = json.load(json_file)
        self.app.pulldb.fetch_page = mock.Mock(return_value=unread)
        self.assertEqual(len(unread['results']), 100)
        results = self.app.pulldb.fetch_unread()
        self.app.pulldb.fetch_page.assert_called_with(
            '/api/pulls/list/unread', cursor=None
        )
        self.assertEqual(len(results), 100)
        for pull in results:
            self.assertTrue('issue_id' in pull)

    def fetch_new_test(self):
        self.app.setup()
        with open(os.path.join(TEST_DATA_DIR, 'new.json')) as json_file:
            unread = json.load(json_file)
        self.app.pulldb.fetch_page = mock.Mock(return_value=unread)
        self.assertEqual(len(unread['results']), 39)
        results = self.app.pulldb.fetch_new()
        self.app.pulldb.fetch_page.assert_called_with(
            '/api/pulls/list/new', cursor=None, cache=False
        )
        self.assertEqual(len(results), 39)
        for pull in results:
            self.assertTrue('issue_id' in pull)
