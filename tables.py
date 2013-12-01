from collections import defaultdict
from operator import itemgetter
from math import log
import sqlite3
import random


class BaseTable:

    def __len__(self):
        return self._tuples

    def __iter__(self):
        for i in xrange(self._tuples):
            yield self._table[i]

    def __getitem__(self, key):
        return self._table[key]

    def to_indices(self, attrs):
        names = map(itemgetter(0), self.attributes)
        return [a if isinstance(a, int) else names.index(a) for a in attrs]


class ListTable(BaseTable):

    def __init__(self, attributes, table):
        self.attributes = attributes
        self._table = table
        self._tuples = len(table)


class SqliteTable(BaseTable):

    def __init__(self, database, tablename, **kwargs):
        database = database if isinstance(database, sqlite3.Connection) else sqlite3.connect(database)
        database.text_factory = str
        cursor = database.cursor()
        query = 'SELECT * FROM %s' % tablename
        
        order_by = kwargs.get('order_by', None)
        if order_by:
            query += ' ORDER BY ' + (order_by if isinstance(order_by, str) else ', '.join(order_by))

        self._table = cursor.execute(query).fetchall()
        self._tuples = len(self._table)
        self.attributes = map(itemgetter(1, 2), cursor.execute('pragma table_info(%s)' % tablename).fetchall())


class BaseGeneratedTable(BaseTable):

    def __init__(self, tuples, attrs, generator):
        self._tuples = tuples
        self._table = defaultdict(lambda: tuple(generator() for _ in xrange(attrs)))
        self.attributes = [('attr_%i' % i, 'INTEGER') for i in xrange(attrs)]


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
