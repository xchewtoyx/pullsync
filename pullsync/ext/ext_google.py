import argparse
import json
import os

from cement.core import handler, hook
import httplib2
from oauth2client import client
from oauth2client import tools
from oauth2client.contrib.multistore_file import get_credential_storage
import xdg.BaseDirectory

from pullsync.ext.interfaces import AuthInterface


class GoogleHandler(handler.CementBaseHandler):
    class Meta:
        interface = AuthInterface
        label = 'google'
        scopes = [
            'https://www.googleapis.com/auth/userinfo.email',
            'https://www.googleapis.com/auth/devstorage.read_write',
        ]
        user_agent = 'pullsync/0.1'

    def _setup(self, app):
        app.log.debug('Setting up google api client')
        super(GoogleHandler, self)._setup(app)
        self.scopes = self.Meta.scopes
        self.app.extend('google', self)
        self._http = None

    @property
    def client_secrets(self):
        secrets_path = os.path.join(
            xdg.BaseDirectory.save_data_path(self.app._meta.label),
            'client_secrets.json')
        with open(secrets_path) as secrets_file:
            client_secret_data = json.load(secrets_file)
        return client_secret_data.get('installed')

    @property
    def credential_store(self):
        storage_path = os.path.join(
            xdg.BaseDirectory.save_data_path(self.app._meta.label),
            'oauth_credentials')
        return get_credential_storage(
            storage_path,
            self.client_secrets['client_id'],
            self.Meta.user_agent,
            self.scopes
        )


    @property
    def client(self):
        if not self._http:
            http_client = httplib2.Http()
            credentials = self.credential_store.get()
            if not credentials or credentials.invalid:
                self.app.log.debug('No valid credentials, authorizing...')
                flow = client.OAuth2WebServerFlow(
                    client_id=self.client_secrets['client_id'],
                    client_secret=self.client_secrets['client_secret'],
                    scope=self.scopes,
                    user_agent=self.Meta.user_agent,
                    redirect_url="urn:ietf:wg:oauth:2.0:oob",
                )
                tools.run_flow(flow, self.credential_store, self.app.pargs)
            self.credential_store.get().authorize(http_client)
            self._http = http_client
        return self._http

    def add_scope(self, scope):
        if scope not in self.scopes:
            self.scopes.append(scope)
            self._http = None


def load_google_args(app):
    if not isinstance(app.args, argparse.ArgumentParser):
        raise TypeError('Cannot add arguments to non argparse parser %r' % (
            app.args))
    app.args._add_container_actions(tools.argparser)
    app.args.set_defaults(noauth_local_webserver=True)


def load(app=None):
    handler.register(GoogleHandler)
    hook.register('pre_argument_parsing', load_google_args)
    google = GoogleHandler()
    hook.register('post_setup', google._setup)
