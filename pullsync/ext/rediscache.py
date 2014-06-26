from cement.core import cache, handler, hook
import redis

class RedisCache(handler.CementBaseHandler):
    class Meta:
        label = 'redis'

    def _setup(self, app):
        self.app = app
        self.app.redis = self
        self.client = redis.StrictRedis(host='localhost', port=6379, db=0)

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
