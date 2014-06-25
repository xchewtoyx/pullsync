import json
import os
import re
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
            }),
        ]

    def normalise_name(self, original_name):
        normal = original_name.lower()
        # Remove the file extension
        normal = re.sub(r'.cb[rz]$', '', normal)
        # tags in brackets can be stripped
        normal = re.sub(r'\([^)]+\)', '', normal)
        # Some substitutions for known bad nameing
        normal = re.sub(r'Garth Ennis\'?', '', normal, flags=re.I)
        # Normalise spaces.
        normal = re.sub(r'[_+]', ' ', normal)
        normal = re.sub(r' +', ' ', normal)
        normal = normal.strip()
        # Strip leading zeroes from issue number
        normal = re.sub(r'0+(\d+)$', r'\g<1>', normal)
        # Try to identify the issue number
        issue_number = re.search(r'(\d+|)$', normal).group(1)
        return normal, issue_number

    def compare_pull(self, candidate, pulls):
        candidate_name, candidate_issue = candidate
        for pull in pulls:
            normalised, pull_issue = self.normalise_name(
                pull.get('name', ''))
            # The Levenshtein distance is the number of edits needed to
            # transform one string into another.  On a small string even
            # a small distance can indicate a large difference between
            # strings, so I weight the distance by string length to
            # try and counter this
            min_length = abs(
                min([len(normalised), len(candidate_name), -1]))
            name_distance = distance(unicode(candidate[0]), unicode(normalised))
            weighted_distance = float(name_distance)/min_length
            yield (
                candidate_issue != pull_issue,
                weighted_distance,
                candidate
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
                yield self.app.redis.get(key)

    @controller.expose(hide=True)
    def default(self):
        candidates = list(self.scan_dir(self.app.pargs.scandir))
        match_cache = []
        for candidate = candidates:
            matches = [
                match for match in self.compare_pull(candidate, pulls)
            ]
            match_cache.append([pull, matches])
            if matches:
                best_match = min(matches)
            else:
                continue
            print '#%8s - %s (%0.4f)' % (pull['pull']['identifier'],
                                         pull['pull']['name'],
                                         best_match[1],
                                     )
            if best_match[1] < 0.8 and not best_match[0]:
                source_file = os.path.join(best_match[2][1], best_match[2][2])
                issue_id = int(pull['pull']['identifier'])
                issue_path = os.path.join(
                    '%2x' % (issue_id & 0xff,),
                    '%2x' % ((issue_id & 0xff00) >> 8,),
                    '%2x' % (issue_id,),
                )
                destination_file = os.path.join(
                    'gs://long-box', issue_path, best_match[2][2]
                )
                print 'gsutil cp "%s" "%s"' % (
                    source_file, destination_file
                )
            else:
                print '# best match does not meet threshold.'
                print '# %r' % (best_match[0:2],)
                print '# %s' % best_match[2][2]
            print
        with open('match_cache.json', 'w') as cache_file:
            json.dump(match_cache, cache_file)

def load():
    handler.register(UploadController)
