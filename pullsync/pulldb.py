import json

from cement.core import controller, handler
from dateutil.parser import parse as parse_date

from pullsync.interfaces import ReadinglistInterface

class PullDB(handler.CementBaseHandler):
    class Meta:
        label = 'pulldb'
        interface = ReadinglistInterface

    def _setup(self, app):
        self.app = app
        self.base_url = self.app.config.get('pulldb', 'base_url')
        auth_handler = handler.get('auth', 'oauth2')()
        auth_handler._setup(self.app)
        self.http_client = auth_handler.client()

    def list_new(self, data_file=None):
        if data_file:
            with open(data_file, 'r') as source:
                response = json.load(source)
        else:
            path = '/api/pulls/list/new?context=1'
            resp, content = self.http_client.request(self.base_url + path)
            if resp.status != 200:
                self.app.log.error(resp, content)
                response = {
                    'status': 500,
                    'results': [],
                }
            else:
                response = json.loads(content)
        return response

    def list_unread(self):
        path = '/api/pulls/unread'
        resp, content = self.http_client.request(self.base_url + path)
        if resp.status != 200:
            self.app.log.error(resp, content)
        else:
            return json.loads(content)

    def _post_list(self, path, list_key, id_list):
        data = json.dumps({
            list_key: id_list,
        })
        resp, content = self.http_client.request(
            self.base_url + path,
            method='POST',
            headers={'Content-Type': 'application/json'},
            body=data,
        )
        if resp.status != 200:
            self.app.log.error('%r %r' % (resp, content))
        else:
            return json.loads(content)

    def add(self, id_list):
        path = '/api/pulls/add'
        list_key = 'issues'
        self._post_list(path, list_key, id_list)

def load():
    handler.register(PullDB)
