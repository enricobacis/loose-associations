from collections import defaultdict
from random import random
from math import log
import sqlite3


class Table(object):

    def __len__(self):
        return self._tuples

    def __iter__(self):
        for i in xrange(self._tuples):
            yield self._table[i]

    def __getitem__(self, key):
        return self._table[key]


class SqliteTable(Table):

    def __init__(self, database, tablename):
        database = database if isinstance(database, sqlite3.Connection) else sqlite3.connect(database)
        database.text_factory = str
        self._table = database.cursor().execute('SELECT * FROM %s' % tablename).fetchall()
        self._tuples = len(self._table)


class RandomTable(Table):

    def __init__(self, tuples, attrs, coeff=0.5, maxvalue=1000):
        self._tuples = tuples
        selfsimilar = lambda: int(maxvalue * (random() ** (log(coeff) / log(1.0 - coeff))))
        self._table = defaultdict(lambda: tuple(selfsimilar() for _ in xrange(attrs)))
