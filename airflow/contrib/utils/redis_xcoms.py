import json
import redis
from airflow.models.xcom import BaseXCom

redis_client = redis.Redis('localhost', 6379)

class RedisXCom(BaseXCom):

    @classmethod
    def set(cls, key, value):
        value = cls.serialize_value(value)
        key = "xcom:" + key
        redis_client.set(key, value)

    @classmethod
    def get(cls, key):
        key = "xcom:" + key
        value = redis_client.get(key)
        value = cls.deserialize_value(value)

    @staticmethod
    def serialize_value(value):
        return json.dumps(value)

    @staticmethod
    def deserialize_value(json_string):
        return json.loads(json_string)