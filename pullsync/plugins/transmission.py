import json
import os
import re
import subprocess
import time

import apiclient
from apiclient.discovery import build
from cement.core import controller, handler
from dateutil.parser import parse as parse_date
from transmissionrpc import Client

class TransmissionController(controller.CementBaseController):
    class Meta:
        label = 'torrent'
        stacked_on = 'base'
        stacked_type = 'nested'
        arguments = [
            (['--torrent'], {
                'help': 'torrent id to scan for files',
                'action': 'store',
                'type': int,
                'required': True,
            }),
            (['--threshold', '-t'], {
                'help': 'Maximum safe Levenshtein distance',
                'action': 'store',
                'type': float,
                'default': 0.2,
            }),
            (['--commit'], {
                'help': 'Mark files that meet threshold for download',
                'action': 'store_true',
            }),
            (['--check_type', '-c'], {
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
        torrent = self.app.pargs.torrent
        fileid, filename = candidate
        self.app.log.info('Marking file for download: %s[%d:%d]' % (
            filename, torrent, fileid
        ))
        self.app.transmission.client.set_files({
            torrent: { fileid: { 'priority': 'normal', 'selected': True} }
        })

    def scan_torrent(self, torrent_id):
        files = self.app.transmission.client.get_files(torrent_id)
        self.app.log.debug('Found file list: %r[%r]' % (files, type(files)))
        for key, detail in files[torrent_id].items():
            yield key, os.path.basename(detail['name'])

    def identify_unseen(self, check_type='unseen'):
        if check_type == 'unseen':
            pulls = self.app.pulldb.list_unseen()
        else:
            pulls = self.app.pulldb.fetch_new()
        for pull in pulls:
            yield pull

    def find_matches(self, candidates, pulls, threshold):
        for candidate in candidates:
            matches = [
                match for match in self.app.match.compare_pull(
                    candidate, pulls)
            ]
            if matches:
                best_match = min(matches)
            else:
                continue
            good_match = bool(
                best_match[1] < threshold and not best_match[0]
            )
            logger = self.app.log.debug
            if good_match:
                logger = self.app.log.info
            logger('Match: %5r <%0.4f> [%s -> %s]' % (
                good_match,
                best_match[1],
                best_match[3][0],
                best_match[4][0]))
            yield good_match, best_match, candidate

    @controller.expose(aliases=['help'], hide=True)
    def default(self):
        self.app.args.print_help()

    @controller.expose(
        help='Scan a torrent file for files matching wanted pulls')
    def scan(self):
        candidates = list(self.scan_torrent(self.app.pargs.torrent))
        self.app.log.debug('Candidate files: %r' % candidates)
        pulls = list(self.identify_unseen(
            check_type=self.app.pargs.check_type))
        self.app.log.debug('Unseen pulls: %r' % pulls)
        for good_match, best_match, candidate in self.find_matches(
                candidates, pulls, self.app.pargs.threshold):
            if good_match and self.app.pargs.commit:
                self.commit_file(best_match[2], candidate)


def load(app=None):
    handler.register(TransmissionController)
