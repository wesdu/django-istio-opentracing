from . import mysql_client
from . import redis_client


def install_all_patch():
    mysql_client.install_patch()
    redis_client.install_patch()
