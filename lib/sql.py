#!/usr/bin/env python3

import sqlite3

class SQLBase():

    # __init__
    #
    def __init__(self, db):
        try:
            self.sql = sqlite3.connect(db)
            self.sql.row_factory = sqlite3.Row
        except Exception as e:
            self.sql.rollback()
            self.sql.close()
            raise e

    def commit(self, query):
        try:
            cursor = self.sql.cursor()
            cursor.execute(query)
            self.sql.commit()
        except Exception as e:
            self.sql.rollback()
            self.sql.close()
            raise e

    def query(self, query):
        cursor = self.sql.cursor()
        cursor.execute(query)
        return cursor

    def fetch_all(self, query):
        results = self.query(query).fetchall()
        return results

    def fetch_one(self, query):
        results = self.query(query).fetchone()
        return results


# vi:set ts=4 sw=4 expandtab syntax=python:
