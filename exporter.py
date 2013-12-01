from operator import itemgetter
from utils import shuffled, placeholders
import sqlite3
import logging


class Exporter:

    def __init__(self, table, fragments, associations):
        self._table = table
        self._fragments = map(table.to_indices, fragments)
        self._associations = associations

    def to_sqlite(self, database):
        '''Export data to sqlite3 database'''
        self._to_db(database if isinstance(database, sqlite3.Connection) else sqlite3.connect(database))

    def _to_db(self, database):
        '''Common method to export data for all the relational databases using DB-API'''
        cursor = database.cursor()

        logging.info('creating the associations table ...')
        cursor.execute('CREATE TABLE associations (%s)' % ', '.join('group_%i INTEGER' % i for i in xrange(len(self._fragments))))
        cursor.execute('CREATE UNIQUE INDEX i_associations ON associations (%s)' % ', '.join('group_%i' % i for i in xrange(len(self._fragments))))
        cursor.executemany('INSERT INTO associations VALUES (%s)' % placeholders(len(self._fragments)), self._associations.values())

        # create the schema table that will be used by the querier to know where the attributes are
        cursor.execute('CREATE TABLE schema (attribute TEXT PRIMARY KEY, fragment INTEGER)')

        for fragment_id, fragment in enumerate(self._fragments):
            # select will extract only the data included in the fragment
            select = itemgetter(*fragment)
            attributes = select(self._table.attributes)
            data = (select(self._table[row]) + (group,) for group, rows in self._associations.get_groups(fragment_id).items() for row in shuffled(rows))

            logging.info('creating the fragment_%i table ...' % fragment_id)
            cursor.execute('CREATE TABLE fragment_{0} ({1}, group_{0} INTEGER)'.format(fragment_id, ', '.join('%s %s' % attr for attr in attributes)))
            cursor.execute('CREATE INDEX i_fragment_{0} ON fragment_{0} (group_{0})'.format(fragment_id))
            cursor.executemany('INSERT INTO fragment_%i VALUES (%s)' % (fragment_id, placeholders(len(fragment) + 1)), data)
            cursor.executemany('INSERT INTO schema VALUES (?, ?)', ((attr[0], fragment_id) for attr in attributes))

        database.commit()
