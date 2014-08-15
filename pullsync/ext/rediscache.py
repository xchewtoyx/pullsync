from cement.core import cache, handler, hook
import redis

from pullsync.ext import interfaces


class RedisCache(handler.CementBaseHandler):
    class Meta:
        label = 'redis'
        interface = interfaces.PullsInterface
        config_defaults = {
            'hostname': 'localhost',
            'port': 6379,
            'database': 0,
        }

    def _setup(self, app):
        super(RedisCache, self)._setup(app)
        self.app.extend('redis', self)
        self.client = None

    def _register_client(self, app):
        self.client = redis.StrictRedis(
            host=self.app.config.get(self._meta.config_section, 'hostname'),
            port=self.app.config.get(self._meta.config_section, 'port'),
            db=self.app.config.get(self._meta.config_section, 'database'),
        )

    def __getattr__(self, attr):
        return getattr(self.client, attr)

    def multi_set(self, items, size=50, ttl=None):
        for index in range(0, len(items), size):
            with self.client.pipeline() as pipe:
                for key, value in items[index:index+size]:
                    if ttl:
                        pipe.setex(key, ttl, value)
                    else:
                        pipe.set(key, value)
                pipe.execute()


def load():
    redis_handler = RedisCache()
    hook.register('post_setup', redis_handler._setup)
    hook.register('post_argument_parsing', redis_handler._register_client)
