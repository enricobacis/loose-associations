from collections import defaultdict
from math import log
import sqlite3
import random


class Table(object):

    def __len__(self):
        return self._tuples

    def __iter__(self):
        for i in xrange(self._tuples):
            yield self._table[i]

    def __getitem__(self, key):
        return self._table[key]

    def to_indices(self, attrs):
        return [self.attributes.index(a) for a in attrs]


class SqliteTable(Table):

    def __init__(self, database, tablename):
        database = database if isinstance(database, sqlite3.Connection) else sqlite3.connect(database)
        database.text_factory = str
        cursor = database.cursor().execute('SELECT * FROM %s' % tablename)
        self._table = cursor.fetchall()
        self._tuples = len(self._table)
        self.attributes = zip(*cursor.description)[0]


class BaseGeneratedTable(Table):

    def __init__(self, tuples, attrs, generator):
        self._tuples = tuples
        self._table = defaultdict(lambda: tuple(generator() for _ in xrange(attrs)))
        self.attributes = map(str, xrange(attrs))


class RandomTable(BaseGeneratedTable):

    def __init__(self, tuples, attrs, maxvalue=100):
        BaseGeneratedTable.__init__(self, tuples, attrs, lambda: random.randint(1, maxvalue))


class SelfSimilarTable(BaseGeneratedTable):

    def __init__(self, tuples, attrs, coeff=0.5, maxvalue=100):
        BaseGeneratedTable.__init__(self, tuples, attrs,
            lambda: int(maxvalue * (random.random() ** (log(coeff) / log(1.0 - coeff)))))


class GaussianTable(BaseGeneratedTable):

    def __init__(self, tuples, attrs, mu=0, sigma=10):
        BaseGeneratedTable.__init__(self, tuples, attrs,
            lambda: int(random.gauss(mu, sigma)))
