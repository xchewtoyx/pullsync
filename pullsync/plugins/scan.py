import json
import os
import re
import time

import apiclient
from apiclient.discovery import build
from cement.core import controller, handler
from dateutil.parser import parse as parse_date
from Levenshtein import distance

class ScanController(controller.CementBaseController):
    class Meta:
        label = 'scan'
        stacked_on = 'base'
        stacked_type = 'nested'

    @controller.expose(aliases=['help'], aliases_only=True)
    def default(self):
        self.app.args.print_help()


class ScanLocal(controller.CementBaseController):
    class Meta:
        label = 'scan_local'
        stacked_on = 'scan'
        stacked_type = 'nested'
        aliases = ['local']
        aliases_only=True
        arguments = [
            (['--newlist'], {
                'help': 'load new entries from file rather than remote api',
                'action': 'store',
                'default': None,
            }),
            (['--scandir', '-d'], {
                'help': 'directory to scan for files',
                'action': 'store',
            }),
            (['--scanfile', '-f'], {
                'help': 'list of file names to check',
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

    def compare_pull(self, pull, candidates):
        normalised, pull_issue = self.normalise_name(
            pull['issue'].get('name', ''))
        for candidate in candidates:
            candidate_name, candidate_issue = candidate[0]
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

    def split_lines(self, filename):
        with open(filename, 'r') as listfile:
            for line in listfile:
                yield os.path.dirname(line), os.path.basename(line).strip()

    def scan_file(self, listfile):
        for path, filename in self.split_lines(listfile):
            if filename.endswith(('.cbr', '.cbz')):
                self.app.log.debug('Found file %s' % filename)
                yield (self.normalise_name(filename), path, filename)

    @controller.expose(hide=True)
    def default(self):
        new_items = self.pulldb.list_unread()
        pulls = new_items['results']
        candidates = []
        if self.app.pargs.scandir:
            candidates = list(self.scan_dir(self.app.pargs.scandir))
        elif self.app.pargs.scanfile:
            candidates = list(self.scan_file(self.app.pargs.scanfile))
        match_cache = []
        for pull in pulls:
            matches = [
                match for match in self.compare_pull(pull, candidates)
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

def with_backoff(original_function):
    def retry_with_backoff(*args, **kwargs):
        backoff = 0.1
        attempt = 0
        while True:
            try:
                return original_function(*args, **kwargs)
            except apiclient.errors.HttpError:
                if attempt < 5:
                    sleep(2**attempt * backoff)
                else:
                    raise

    return retry_with_backoff

class ScanRemote(controller.CementBaseController):
    class Meta:
        label = 'scan_remote'
        stacked_on = 'scan'
        stacked_type = 'nested'
        aliases = ['remote']
        aliases_only = True

    @with_backoff
    def check_prefix(self, gsclient, pull_id):
        prefix = 'comics/%02x/%02x/%x' % (
            pull_id & 0xff,
            (pull_id & 0xff00) >> 8,
            pull_id
        )
        if self.app.redis.client.sismember('gs:seen', pull_id):
            file_detail = self.app.redis.client.get('gs:file:%d' % pull_id)
            return json.loads(file_detail)
        request = gsclient.objects().list(bucket='long-box', prefix=prefix)
        response = request.execute()
        if 'items' in response:
            self.app.log.debug('Files found for prefix %s' % prefix)
            file_list = response['items']
            self.app.redis.client.sadd('gs:seen', pull_id)
            self.app.redis.client.set(
                'gs:file:%d' % pull_id, json.dumps(file_list))
            return file_list

    @controller.expose()
    def default(self):
        unread_items = self.app.pulldb.list_unread()

        gsclient = build('storage', 'v1', http=self.app.google.client)

        cache = []
        for pull in unread_items:
            pull_detail = json.loads(self.app.redis.client.get(pull))
            pull_id = int(pull_detail['id'])
            pull_matches = self.check_prefix(gsclient, pull_id)
            if pull_matches:
                print 'File found for [%s] %s' % (
                    pull_detail['identifier'], pull_detail['name'])
                cache.append([pull, pull_matches])
                for item in pull_matches:
                    print item['mediaLink']
            else:
                print 'No match for %s' % pull_detail['identifier']
        with open('cache.json', 'w') as cache_file:
            json.dump(cache, cache_file)

def load():
    handler.register(ScanController)
    handler.register(ScanLocal)
    handler.register(ScanRemote)
