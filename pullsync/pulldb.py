import json
import os

from cement.core import controller, handler
from dateutil.parser import parse as parse_date
import xdg

from pullsync.interfaces import ReadinglistInterface

class FetchError(Exception):
    pass

class PullDB(handler.CementBaseHandler):
    class Meta:
        label = 'pulldb'
        interface = ReadinglistInterface

    def _setup(self, app):
        self.app = app
        self.base_url = self.app.config.get('pulldb', 'base_url')
        self.data_dir = xdg.BaseDirectory.save_data_path(
            self.app._meta.label)
        self.new_file = os.path.join(self.data_dir, 'new_pulls.json')
        self.unread_file = os.path.join(self.data_dir, 'unread_pulls.json')
        auth_handler = handler.get('auth', 'oauth2')()
        auth_handler._setup(self.app)
        self.http_client = auth_handler.client()

    def fetch_new(self, data_file=None):
        if data_file:
            with open(data_file, 'r') as source:
                response = json.load(source)
        else:
            path = '/api/pulls/list/new'
            resp, content = self.http_client.request(self.base_url + path)
            if resp.status != 200:
                self.app.log.error(resp, content)
                raise FetchError('Unable to fetch unread pulls')
            else:
                with open(self.new_file, 'w') as new_pulls:
                    new_pulls.write(content)

    def fetch_unread(self):
        path = '/api/pulls/list/unread'
        resp, content = self.http_client.request(self.base_url + path)
        if resp.status != 200:
            self.app.log.error(resp, content)
            raise FetchError('Unable to fetch unread pulls')
        else:
            with open(self.unread_file, 'w') as unread_pulls:
                unread_pulls.write(content)

    def list_new(self):
        with open(self.new_file, 'r') as new_pulls:
            new_pulls = json.load(new_pulls)
        return new_pulls

    def list_unread(self):
        with open(self.unread_file, 'r') as unread_pulls:
            unread_pulls = json.load(unread_pulls)
        return unread_pulls

def load():
    handler.register(PullDB)
