import json
import os
import re
import time

from cement.core import controller, handler, hook
from dateutil.parser import parse as parse_date
from Levenshtein import distance

from pullsync.ext import interfaces


class MatchHandler(handler.CementBaseHandler):
    class Meta:
        label = 'matcher'
        interface = interfaces.MatchInterface

    def _setup(self, app):
        super(MatchHandler, self)._setup(app)
        self.app.extend('match', self)

    def normalise_name(self, original_name):
        # lowercase and change space characters
        normal = original_name.lower()
        normal = re.sub(r'[_+]', ' ', normal)
        # Remove the file extension
        normal = re.sub(r'.cb[rz]$', '', normal)
        # tags in brackets can be stripped
        normal = re.sub(r'\([^)]+\)', '', normal)
        normal = re.sub(r'\[[^)]+\]', '', normal)
        # Somtimes a random volume number slips in
        normal = re.sub(r'\bv\d+\b', '', normal)
        # Some substitutions for known bad nameing
        normal = re.sub(r'2000ad', '2000 ad', normal)
        normal = re.sub(r'(abe sapien \d+) -.*', r'\g<1>', normal)
        normal = re.sub(r'(b.p.r.d. hell on earth \d+) -.*', r'\g<1>', normal)
        normal = re.sub(r'digital exclusives edition', '', normal)
        normal = re.sub(r'garth ennis\'?', '', normal)
        normal = re.sub(r'george romero\'?s', '', normal)
        normal = re.sub(r'(outcast) by kirkman & azaceta', r'\g<1>', normal)
        normal = re.sub(r'the blood queen', r'blood queen', normal)
        normal = re.sub(r'the black bat', r'black bat', normal)
        normal = re.sub(r'the devilers', r'devilers', normal)
        normal = re.sub(r'robin rises omega', r'robin rises', normal)
        normal = re.sub(r'^trinity of sin - the phantom stranger',
                        'the phantom stranger', normal)
        normal = re.sub(r'by kirkman & azaceta', '', normal)
        # Normalise spaces.
        normal = re.sub(r'[_+]', ' ', normal)
        normal = re.sub(r' +', ' ', normal)
        normal = normal.strip()
        # Fixup issue numbers
        # Rewrite unicode half to 0.5
        normal = re.sub(u'\xbd', r'0.5', normal)
        # Remove random issue suffixes that Marvel seem to like
        normal = re.sub(r'([.\d]+)(?:\.now)$', r'\g<1>', normal)
        # Strip leading zeroes from issue number
        normal = re.sub(r'\b0+([.\d]+)$', r'\g<1>', normal)
        # Try to identify the issue number
        issue_number = re.search(r'(\d+|)$', normal).group(1)
        return normal, issue_number

    def weighted_distance(self, pull_name, candidate_name):
        # The Levenshtein distance is the number of edits needed to
        # transform one string into another.  On a small string even
        # a small distance can indicate a large difference between
        # strings, so I weight the distance by string length to
        # try and counter this
        min_length = min([len(pull_name), len(candidate_name)])
        min_length = max([min_length, 1])
        name_distance = distance(
            unicode(candidate_name),
            unicode(pull_name),
        )
        return float(name_distance)/min_length

    def compare_pull(self, candidate, pulls):
        location, filename = candidate
        candidate_name, candidate_issue = self.normalise_name(filename)
        for pull in pulls:
            try:
                normalised, pull_issue = self.normalise_name(pull['name'])
            except KeyError:
                self.app.log.error('Error: pull %r has no name.' % pull)
                continue
            weighted_distance = self.weighted_distance(normalised,
                                                       candidate_name)
            yield (
                candidate_issue != pull_issue,
                weighted_distance,
                pull,
                (candidate_name, candidate_issue),
                (normalised, pull_issue),
            )


def load(app=None):
    handler.register(MatchHandler)
    hook.register('post_setup', MatchHandler()._setup)
