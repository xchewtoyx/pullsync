import json

from cement.core import controller, handler
from dateutil.parser import parse as parse_date
import xdg

class FetchPulls(controller.CementBaseController):
    class Meta:
        label = 'fetch'
        stacked_on = 'base'
        stacked_type = 'nested'

    @controller.expose(hide=True)
    def default(self):
        self.pulldb.fetch_unread()

def load():
    handler.register(FetchPulls)
