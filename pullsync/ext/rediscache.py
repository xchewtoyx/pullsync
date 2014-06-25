from cement.core import cache, handler, hook
import redis

class RedisCache(handler.CementBaseHandler):
    class Meta:
        interface = cache.ICache
        label = 'redis'

    def _setup(self, app):
        self.app = app
        self.redis_client = redis.StrictRedis(
            host='localhost', port=6379, db=0)
        self.app.redis = self

    @property
    def client(self):
        return self.redis_client

    def delete(self, key):
        return self.redis_client.delete(key)

    def get(self, key):
        return self.redis_client.get(key)

    def multi_set(self, items, size=50, ttl=None):
        for index in range(0, len(items), size):
            with self.redis_client.pipeline() as pipe:
                for key, value in items[index:index+size]:
                    pipe.set(key, value)
                    if ttl:
                        pipe.expire(key, ttl)
                pipe.execute()

    def purge(self, key):
        raise NotImplementedError('Purge is not supported')

    def set(self, key, value):
        return self.redis_client.set(key, value)

def load():
    handler.register(RedisCache)
    redis_handler = RedisCache()
    hook.register('post_setup', redis_handler._setup)
