import redis

class CacheHandler:

    def __init__(self):
        self.connection = redis.Redis(host='0.0.0.0', port=6379, db=0, password='6r!7n3y0')

    def get_info(self, token_name):
        cached_token = None
        try:
            cached_token = self.connection.get(name=token_name)
        except Exception:
            return cached_token

        return cached_token

    def set_info(self, token_info):
        cached_token = None
        try:
            self.connection.set(name=token_info['token_name'], value=token_info['token_value'], ex=token_info['token_expire'])
        except Exception:
            return cached_token
