import json
import os
import sys

from cement.core import foundation, handler
from cement.utils import test
import mock

from pullsync.ext import ext_matcher

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def datafile(name):
    return os.path.join(TEST_DATA_DIR, name)


class TestApp(foundation.CementApp):
    class Meta:
        label = 'pullsync'
        argv = []
        config_files = [
        ]
        extensions = [
            # interfaces must go first
            'pullsync.ext.interfaces',
            'pullsync.ext.ext_matcher',
        ]
        plugin_bootstrap = 'pullsync.plugins'


class UploadPluginTest(test.CementTestCase):
    app_class = TestApp

    def load_extension_test(self):
        self.app.setup()
        self.assertIsInstance(self.app.match, ext_matcher.MatchHandler)

    def normalise_name_test(self):
        self.app.setup()
        self.assertEqual(
            self.app.match.normalise_name('The Walking Dead #012 (2014)'),
            ('the walking dead #12', '12'),
        )

    def weighted_distance_test(self):
        self.app.setup()
        self.assertEqual(
            self.app.match.weighted_distance('superman', 'superman'), 0)
        self.assertGreater(
            self.app.match.weighted_distance('spider-man', 'supergirl'), 0.5)

    def compare_pull_test(self):
        self.app.setup()
        results = json.load(open(datafile('fetch_new.json')))
        pulls = [entry['pull'] for entry in results['results']]

        matches = sorted(list(self.app.match.compare_pull(
            ('.', 'Test Issue 1 (2014).cbr'), pulls)))
        self.assertFalse(matches[0][0])
        self.assertEqual(matches[0][1], 0)
        self.assertEqual(matches[0][2]['id'], '1000')

        matches = sorted(list(self.app.match.compare_pull(
            ('.', 'Test Issue 5'), pulls)))
        self.assertEqual(matches[0][0], False)
        self.assertEqual(matches[0][1], 0)
        self.assertEqual(matches[0][2]['id'], '1004')

        matches = sorted(list(self.app.match.compare_pull(
            ('.', 'dummy 5'), pulls)))
        self.assertEqual(matches[0][0], False)
        self.assertGreater(matches[0][1], 0)

        del pulls[0]['name']
        matches = sorted(list(self.app.match.compare_pull(
            (('test issue 5', '5'), 'dummy_path'), pulls)))
        self.assertEqual(len(matches), 4)

        matches = sorted(list(self.app.match.compare_pull(
            (('test issue 5', '5'), 'dummy_path'), [])))
        self.assertListEqual(matches, [])
