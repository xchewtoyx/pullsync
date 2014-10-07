import json
import os
import re
import subprocess
import time

import apiclient
from apiclient.discovery import build
from cement.core import controller, handler
from dateutil.parser import parse as parse_date


class UploadController(controller.CementBaseController):
    class Meta:
        label = 'upload'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['--scandir', '-d'], {
                'help': 'directory to scan for files',
                'action': 'store',
                'required': True,
            }),
            (['--threshold'], {
                'help': 'Maximum safe Levenshtein distance',
                'action': 'store',
                'type': float,
                'default': 0.25,
            }),
            (['--commit'], {
                'help': 'Perform uploads for files that meet threshold',
                'action': 'store_true',
            }),
            (['--check_type'], {
                'help': 'Check for new pulls, or cached unseen pulls',
                'choices': ['new', 'unseen'],
                'default': 'unseen'
            }),
        ]

    def _pull_if_new(self, best_match):
        pull_id = int(best_match['identifier'])
        if best_match['pulled'] == 'False':
            self.app.pulldb.pull_new(pull_id)

    def commit_file(self, best_match, candidate):
        pull_id = int(best_match['identifier'])
        detail = self.app.longbox.check_prefix(pull_id)
        if detail:
            self._pull_if_new(best_match)
            self.app.log.info('Pull %d has already been uploaded, skipping' % (
                pull_id,))
        else:
            try:
                self.send_file(candidate, best_match)
            except subprocess.CalledProcessError as error:
                self.app.log.error('Error copying file: %r' % error)
            else:
                self.app.longbox.check_prefix(pull_id)
                self._pull_if_new(best_match)

    def scan_dir(self, directory):
        for path, subdirs, files in os.walk(directory):
            for filename in files:
                if filename.endswith(('.cbr', '.cbz')):
                    yield (path, filename)

    def identify_unseen(self, check_type='unseen'):
        if check_type == 'unseen':
            pulls = self.app.pulldb.list_unseen()
        else:
            pulls = self.app.pulldb.fetch_new()
        for pull in pulls:
            yield pull

    def send_file(self, candidate, pull):
        filename = os.path.join(
            candidate[0], candidate[1]
        )
        pull_id = int(pull['identifier'])
        destination = 'gs://long-box/comics/%02x/%02x/%x/' % (
            pull_id & 0xff,
            (pull_id & 0xff00) >> 8,
            pull_id,
        )
        self.app.log.info('Uploading file %r -> %r' % (
            filename, destination
        ))
        subprocess.check_call([
            'gsutil', 'cp', filename, destination,
        ])

    def find_matches(self, candidates, pulls, threshold):
        for candidate in candidates:
            matches = [match for match in self.app.match.compare_pull(
                candidate, pulls)]
            if matches:
                best_match = min(matches)
            else:
                continue
            good_match = bool(
                best_match[1] < threshold and not best_match[0]
            )
            self.app.log.debug('Match: %5r <%0.4f> [%s -> %s]' % (
                good_match,
                best_match[1],
                best_match[3][0],
                best_match[4][0]))
            yield good_match, best_match, candidate

    @controller.expose(hide=True)
    def default(self):
        candidates = list(self.scan_dir(self.app.pargs.scandir))
        self.app.log.debug('Candidate files: %r' % candidates)
        pulls = list(self.identify_unseen(
            check_type=self.app.pargs.check_type))
        self.app.log.debug('Unseen pulls: %r' % pulls)
        for good_match, best_match, candidate in self.find_matches(
                candidates, pulls, self.app.pargs.threshold):
            if good_match and self.app.pargs.commit:
                self.commit_file(best_match[2], candidate)


def load():
    handler.register(UploadController)
