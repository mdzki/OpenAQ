import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "openaq.db")
DB_PATH = os.path.abspath(DB_PATH)


def get_connection():
    return sqlite3.connect(DB_PATH)
