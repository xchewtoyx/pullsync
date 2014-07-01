import json
import os
import re
import subprocess
import time

import apiclient
from apiclient.discovery import build
from cement.core import controller, handler
from dateutil.parser import parse as parse_date
from Levenshtein import distance

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
                'default': 0.2,
            }),
            (['--commit'], {
                'help': 'Perform uploads for files that meet threshold',
                'action': 'store_true',
            }),
        ]

    def normalise_name(self, original_name):
        normal = original_name.lower()
        # Remove the file extension
        normal = re.sub(r'.cb[rz]$', '', normal)
        # tags in brackets can be stripped
        normal = re.sub(r'\([^)]+\)', '', normal)
        normal = re.sub(r'\[[^)]+\]', '', normal)
        # Somtimes a random volume number slips in
        normal = re.sub(r'\bv\d+\b', '', normal)
        # Some substitutions for known bad nameing
        normal = re.sub(r'2000ad', '2000 ad', normal)
        normal = re.sub(r'the black bat', 'black bat', normal)
        normal = re.sub(r'digital exclusives edition', '', normal)
        normal = re.sub(r'garth ennis\'?', '', normal)
        normal = re.sub(r'george romero\'?s', '', normal)
        normal = re.sub(r'^trinity of sin - the phantom stranger',
                        'the phantom stranger', normal)
        normal = re.sub(r'(abe sapien \d+) -.*', r'\g<1>', normal)
        # Normalise spaces.
        normal = re.sub(r'[_+]', ' ', normal)
        normal = re.sub(r' +', ' ', normal)
        normal = normal.strip()
        # Fixup issue numbers
	# Remove random issue suffixes that Marvel seem to like
        normal = re.sub(r'([.\d]+)(?:\.now)$', r'\g<1>', normal)
        # Strip leading zeroes from issue number
        normal = re.sub(r'\b0+([.\d]+)$', r'\g<1>', normal)
        # Try to identify the issue number
        issue_number = re.search(r'(\d+|)$', normal).group(1)
        return normal, issue_number

    def compare_pull(self, candidate, pulls):
        candidate_name, candidate_issue = candidate[0]
        for pull in pulls:
            try:
                normalised, pull_issue = self.normalise_name(pull['name'])
            except KeyError:
                self.app.log.error('Error: pull %r has no name.')
                continue
            # The Levenshtein distance is the number of edits needed to
            # transform one string into another.  On a small string even
            # a small distance can indicate a large difference between
            # strings, so I weight the distance by string length to
            # try and counter this
            min_length = min([len(normalised), len(candidate_name)])
            min_length = max([min_length, 1])
            name_distance = distance(
                unicode(candidate_name),
                unicode(normalised),
            )
            weighted_distance = float(name_distance)/min_length
            yield (
                candidate_issue != pull_issue,
                weighted_distance,
                pull,
                (candidate_name, candidate_issue),
                (normalised, pull_issue),
            )

    def scan_dir(self, directory):
        for path, subdirs, files in os.walk(directory):
            for filename in files:
                if filename.endswith(('.cbr', '.cbz')):
                    yield (self.normalise_name(filename), path, filename)

    def identify_unseen(self):
        unread = self.app.pulldb.list_unread()
        for key in unread:
            dummy, pull_id = key.split(':')
            if not self.app.redis.client.sismember('gs:seen', int(pull_id)):
                yield json.loads(self.app.redis.get(key))

    def send_file(self, candidate, pull):
        filename = os.path.join(
            candidate[1], candidate[2]
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

    @controller.expose(hide=True)
    def default(self):
        candidates = list(self.scan_dir(self.app.pargs.scandir))
        pulls = list(self.identify_unseen())
        match_cache = []
        for candidate in candidates:
            matches = [
                match for match in self.compare_pull(candidate, pulls)
            ]
            match_cache.append([candidate, matches])
            if matches:
                best_match = min(matches)
            else:
                continue
            good_match = bool(
                best_match[1] < self.app.pargs.threshold and not best_match[0]
            )
            self.app.log.debug( 'Match: %5r <%0.4f> [%s -> %s]' % (
                good_match,
                best_match[1],
                best_match[3][0],
                best_match[4][0],
                )
            )
            if good_match and self.app.pargs.commit:
                try:
                    self.send_file(candidate, best_match[2])
                except subprocess.CalledProcessError as error:
                    self.app.log.error('Error copytihg file: %r' % error)

def load():
    handler.register(UploadController)
