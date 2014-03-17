import sqlite3
import logging
import re


class Querier:

    def __init__(self, database, logger=logging.info):
        self._logger = logger or id
        self.database = sqlite3.connect(database) if isinstance(database, str) else database
        self.database.isolation_level = None
        self.database.text_factory = str
        self._cursor = self.database.cursor()
        self._schema = self._cursor.execute('SELECT * FROM schema').fetchall()
        self._get_attributes = re.compile(r'\?(\w+)').findall

    def _create_from_clause(self, fragments):
        if not fragments:
            return ''
        elif len(fragments) == 1:
            return 'fragment_%i' % fragments.pop()
        else:
            return 'associations AS A LEFT JOIN ' + ' LEFT JOIN '.join(
                'fragment_{0} AS F{0} ON A.group_{0} = F{0}.group_{0}'.format(fragment) for fragment in fragments)

    def _get_joined_table(self, fragments):
        if not fragments:
            return ''
        
        joined = 'joined_' + '_'.join(map(str, fragments))

        # check if the joined table already exists (sqlite-dependant)
        if not self._cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'" % joined).fetchall():
            logging.info('creating joined table %s' % joined)
            attributes = filter(lambda attr: attr[2] in fragments, self._schema)
            self._cursor.execute('CREATE TABLE {} ({})'.format(joined,
                ', '.join('%s %s' % (attribute, typ) for attribute, typ, fragment in attributes)))
            self._cursor.execute('INSERT INTO {0} ({1}) SELECT {1} FROM {2}'.format(joined,
                ', '.join('%s' % attribute for attribute, typ, fragment in attributes), self._create_from_clause(fragments)))
        return joined

    def query(self, sql):
        '''Parse the query and expand the question-marked syntax'''
        # select the fragments in which the question-marked attributes are stored
        attributes = self._get_attributes(sql)
        fragments = set(fragment for (attribute, typ, fragment) in self._schema if attribute in attributes)
        tables = self._get_joined_table(fragments)

        # replace question-marked syntax with the generated sql syntax
        sql = ' '.join(sql.replace('? ', tables + ' ').replace('?', '').split())
        self._logger(sql)
        return self._cursor.execute(sql).fetchall()

    def print_query(self, *args):
        for t in self.query(*args):
            print ' | '.join(str(x) for x in t)


def main():
    from argparse import ArgumentParser

    parser = ArgumentParser(description='Query the loose-associations database easily.')
    parser.add_argument('database', help='loose-associations database')
    args = parser.parse_args()

    print 'Enter your queries using ? before loose attributes and \'FROM ?\' to auto-join.'
    print 'example:  SELECT ?disease FROM ? WHERE ?name = "Alice"'
    print 'Enter a blank line to exit.'

    querier = Querier(args.database)

    while True:
        query = raw_input('> ')
        if query == '':
            break
        try:
            querier.print_query(query)
        except Error, e:
            print 'An error occurred:', e.args[0]

if __name__ == '__main__':
    main()
