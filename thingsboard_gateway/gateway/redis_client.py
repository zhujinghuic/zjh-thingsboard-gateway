from threading import Lock

import redis


class RedisClient:
    __redis_op = None

    @staticmethod
    def init_redis_pool(__config):
        pass
            # RedisClient.__pool = redis.ConnectionPool(host=__config['redis'].get('host'), password=__config['redis'].get('password'), decode_responses=True)

    @classmethod
    def get_redis_client(cls):
        # r = redis.Redis(connection_pool=pool)
        if not cls.__redis_op:
            with Lock():
                if not cls.__redis_op:
                    __pool = redis.ConnectionPool(host='192.168.88.108', password='', decode_responses=True)
                    cls.__redis_op = redis.Redis(connection_pool=__pool)
        return cls.__redis_op
