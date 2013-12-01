import sqlite3
import logging
import re


class Querier:

    def __init__(self, database):
        self._database = database if isinstance(database, sqlite3.Connection) else sqlite3.connect(database)
        self._database.isolation_level = None
        self._database.text_factory = str
        self._cursor = self._database.cursor()
        self._schema = self._cursor.execute('SELECT * FROM schema').fetchall()
        self._get_attributes = re.compile(r'\?(\w+)').findall

    def query(self, sql):
        '''Parse the query and expand the question-marked syntax'''
        # select the fragments in which are stored the question-marked attributes
        attributes = self._get_attributes(sql)
        fragments = set(fragment for (attribute, fragment) in self._schema if attribute in attributes)

        # create the FROM clause using question-marked fragments and join them when necessary
        if not fragments:
            tables = ''
        elif len(fragments) == 1:
            tables = 'fragment_%i' % fragments.pop()
        else:
            tables = 'associations AS A LEFT JOIN ' + ' LEFT JOIN '.join(
                'fragment_{0} AS F{0} ON A.group_{0} = F{0}.group_{0}'.format(fragment) for fragment in fragments)

        # replace question-marked syntax with the generated sql syntax
        sql = ' '.join(sql.replace('? ', tables + ' ').replace('?', '').split())

        logging.info(sql)
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
        except sqlite3.Error as e:
            print 'An error occurred:', e.args[0]

if __name__ == '__main__':
    main()
