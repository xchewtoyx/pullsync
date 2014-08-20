import os

from cement.core import foundation, controller


class BaseController(controller.CementBaseController):
    class Meta:
        label = 'base'

    @controller.expose(aliases=['help'], aliases_only=True)
    def default(self):
        self.app.args.print_help()


class PullsyncApp(foundation.CementApp):
    class Meta:
        label = 'pullsync'
        base_controller = BaseController
        extensions = [
            # interfaces must go first
            'pullsync.ext.interfaces',
            'pullsync.ext.ext_google',
            'pullsync.ext.ext_longbox',
            'pullsync.ext.ext_matcher',
            'pullsync.ext.ext_pulldb',
            'pullsync.ext.ext_redis',
        ]


def run():  # pragma: no cover
    app = PullsyncApp()
    try:
        app.setup()
        app.run()
    finally:
        app.close()


if __name__ == '__main__':  # pragma: no cover
    main()
