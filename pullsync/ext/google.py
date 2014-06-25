import argparse
import json
import os

from cement.core import handler, hook
import httplib2
from oauth2client import client
from oauth2client import tools
from oauth2client.multistore_file import get_credential_storage
import xdg.BaseDirectory

from pullsync.ext.interfaces import AuthInterface

class GoogleHandler(handler.CementBaseHandler):
    class Meta:
        interface = AuthInterface
        label = 'google'
        scope = (
            'https://www.googleapis.com/auth/userinfo.email '
            'https://www.googleapis.com/auth/devstorage.read_write '
        )
        user_agent = 'pullsync/0.1'

    def _setup(self, app):
        app.log.info('Setting up google api client')
        self.app = app
        self.app.google = self

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
            self.Meta.scope
        )

    @property
    def client(self):
        http_client = httplib2.Http()
        credentials = self.credential_store.get()
        if not credentials or credentials.invalid:
            self.app.log.debug('No valid credentials, authorizing...')
            flow = client.OAuth2WebServerFlow(
                client_id=self.client_secrets['client_id'],
                client_secret=self.client_secrets['client_secret'],
                scope=self.Meta.scope,
                user_agent=self.Meta.user_agent,
                redirect_url="urn:ietf:wg:oauth:2.0:oob",
            )
            tools.run_flow(flow, self.credential_store, self.app.pargs)
        self.credential_store.get().authorize(http_client)
        return http_client

def load_google_args(app):
    if not isinstance(app.args, argparse.ArgumentParser):
        raise TypeError('Cannot add arguments no non argparse parser %r' % (
            app.args))
    app.args._add_container_actions(tools.argparser)
    app.args.set_defaults(noauth_local_webserver=True)

def load():
    handler.register(GoogleHandler)
    hook.register('pre_argument_parsing', load_google_args)
    google = GoogleHandler()
    hook.register('post_setup', google._setup)
