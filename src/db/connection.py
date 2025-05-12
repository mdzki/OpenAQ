import sqlite3


def get_connection(path="openaq.db"):
    return sqlite3.connect(path)