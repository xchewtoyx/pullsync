from cement.core import handler, interface


class AuthInterface(interface.Interface):
    class IMeta:
        label = 'auth'

    def _setup(app):
        pass


class PullsInterface(interface.Interface):
    class IMeta:
        label = 'pulls'

    def _setup(app):
        pass


class ReadinglistInterface(interface.Interface):
    class IMeta:
        label = 'readinglist'

    def _setup(app):
        pass


def load():
    handler.define(AuthInterface)
    handler.define(PullsInterface)
    handler.define(ReadinglistInterface)
