from cement.core import cache, handler, hook

from pullsync.ext import interfaces
from transmissionrpc import Client

class TransmissionClient(handler.CementBaseHandler):
    class Meta:
        label = 'transmission'
        interface = interfaces.DataInterface
        config_defaults = {
            'hostname': 'localhost',
            'port': 9091,
        }

    def _setup(self, app):
        super(TransmissionClient, self)._setup(app)
        self.app.extend('transmission', self)
        self.client = None

    def _register_client(self, app):
        self.client = Client(
            address=self.app.config.get(self._meta.config_section, 'hostname'),
            port=self.app.config.get(self._meta.config_section, 'port'),
        )

    def __getattr__(self, attr):
        return getattr(self.client, attr)


def load(app=None):
    transmission_handler = TransmissionClient()
    hook.register('post_setup', transmission_handler._setup)
    hook.register('post_argument_parsing',
                  transmission_handler._register_client)
