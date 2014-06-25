import argparse
import json
import os
from urllib import urlencode

from cement.core import foundation, controller

from pullsync import interfaces

class BaseController(controller.CementBaseController):
    class Meta:
        label = 'base'

    @controller.expose(aliases=['help'], aliases_only=True)
    def default(self):
        self.app.args.print_help()

def run():
    app = foundation.CementApp(
        label='pullsync', base_controller=BaseController,
        extensions=[
            'pullsync.ext.interfaces',
            'pullsync.ext.pulldb',
            'pullsync.ext.google',
            'pullsync.ext.rediscache',
        ]
    )
    try:
        app.setup()
        app.run()
    finally:
        app.close

if __name__ == '__main__':
    main()
