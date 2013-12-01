from associations import Associations
from constraints import Constraints
from collections import Counter
from random import random
from utils import neighbours
from time import time
import logging


class Loose:

    def __init__(self, table, constraints, fragments):
        self._table = table
        self._fragments = map(table.to_indices, fragments)
        self._constraints = Constraints(map(table.to_indices, constraints), self._fragments)

    def _get_group_data(self, fragment_id, group_id):
        return ((row_id, self._table[row_id]) for row_id in self._associations.get_group(fragment_id, group_id))

    def _get_nonfull_groups(self, fragment_id, allow_skips=True):
        return (group_id for group_id in xrange(self._first_nonfull[fragment_id], self._max_groups[fragment_id])
                if not self._is_group_full(fragment_id, group_id)
                if not allow_skips or not self._skip_probability or random() > self._skip_probability)

    def _is_group_full(self, fragment_id, group_id):
        return self._associations.get_group_size(fragment_id, group_id) >= self._k_list[fragment_id]

    def _are_groups_alike(self, group1, group2, fragment_id, constraint_id):
        return any(self._constraints.are_rows_alike_for(row1, row2, fragment_id, constraint_id)
                   for _, row1 in self._get_group_data(fragment_id, group1)
                   for _, row2 in self._get_group_data(fragment_id, group2))

    def _check_group_heterogenity(self, row, fragment_id, group_id):
        return not any(self._constraints.are_rows_alike(row, other_row, fragment_id)
                       for _, other_row in self._get_group_data(fragment_id, group_id))

    def _check_association_heterogenity(self, association, fragment_id):
        return not any(self._associations.exists(fragment_id, association[fragment_id], other_fragment, other_group)
                       for other_fragment, other_group in enumerate(association)
                       if other_fragment != fragment_id)

    def _check_deep_heterogenity(self, row, association, fragment_id):
        return all(all(all(any(not self._are_groups_alike(association[frag_2], other_association[frag_2], frag_2, constraint_id)
                               for frag_2 in self._constraints.involved_fragments_for(constraint_id)
                               if frag_2 != frag_1)
                           for other_association in self._associations.get_associated(frag_1, association[frag_1]))
                       for frag_1 in self._constraints.involved_fragments_for(constraint_id))
                   for constraint_id in self._constraints.completed_constraints_for(fragment_id, len(association)))

    def _check_heterogenity(self, row, association, fragment_id):
        return (self._check_group_heterogenity(row, fragment_id, association[fragment_id]) and
                self._check_association_heterogenity(association, fragment_id) and
                self._check_deep_heterogenity(row, association, fragment_id))

    def _extend_association(self, row, association=[]):
        fragment_id = len(association)
        if (fragment_id == len(self._fragments)):
            return association

        for group_id in self._get_nonfull_groups(fragment_id):
            if self._check_heterogenity(row, association + [group_id], fragment_id):
                result = self._extend_association(row, association + [group_id])
                if result is not None:
                    return result

    def _full_neighbours_groups(self, fragment_id, group_id):
        return (new_group_id for new_group_id in neighbours(group_id, 0, self._max_groups[fragment_id])
                if self._is_group_full(fragment_id, new_group_id))

    def _redistribute_group(self, fragment_id, group_id):
        logging.debug('redistributing group {} in fragment {}'.format(group_id, fragment_id))
        for row_id, row in self._get_group_data(fragment_id, group_id):
            association = list(self._associations[row_id])
            if association:
                for new_group_id in self._full_neighbours_groups(fragment_id, group_id):
                    association[fragment_id] = new_group_id
                    if self._check_heterogenity(row, association, fragment_id):
                        logging.debug('fragment {} row {}: group {} -> group {}'.format(fragment_id, row_id, group_id, new_group_id))
                        self._associations[row_id] = association
                        break
                else:
                    logging.debug('fragment {} row {} : group {} -> not reallocable'.format(fragment_id, row_id, group_id))
                    self._delete_row(row_id)

    def _delete_row(self, row_id):
        self._dropped.append(row_id)
        association = self._associations[row_id]
        logging.debug('deleting row {} = {}'.format(row_id, association))
        del self._associations[row_id]
        for fragment_id, group_id in enumerate(association):
            if not self._is_group_full(fragment_id, group_id):
                self._redistribute_group(fragment_id, group_id)
        logging.debug('row {} deleted'.format(row_id))

    def _update_first_nonnull(self, association):
        for fragment_id, group_id in enumerate(association):
            if group_id == self._first_nonfull[fragment_id]:
                while (self._is_group_full(fragment_id, self._first_nonfull[fragment_id])):
                    self._first_nonfull[fragment_id] += 1

    def associate(self, k_list, skip_probability=0, **kwargs):
        print_stats_every = kwargs.get('print_stats_every', 1000)
        more_groups = kwargs.get('more_groups', 5)
        self._k_list = k_list
        self._max_groups = [len(self._table) / k + more_groups for k in self._k_list]
        self._first_nonfull = [0 for _ in xrange(len(self._fragments))]
        self._associations = Associations(len(self._fragments))
        self._dropped = []
        self._skip_probability = skip_probability
        started = time()

        for row_id, row in enumerate(self._table):
            if row_id and not row_id % print_stats_every:
                logging.info('[{:.2f}s] associating row: {} using first_nonfull: {}'.format(time() - started, row_id, self._first_nonfull))

            association = self._extend_association(row)
            if association is None:
                self._dropped.append(row_id)
                logging.warning('row_id {} dropped at first scan. You should increase _max_groups.'.format(row_id))
            else:
                self._associations[row_id] = association
                self._update_first_nonnull(association)

        for fragment_id in xrange(len(self._fragments)):
            for group_id in self._get_nonfull_groups(fragment_id, False):
                self._redistribute_group(fragment_id, group_id)

        return self._associations, self._dropped

    def associate_with_retries(self, k_list, retries, skip_probability=0.05, **kwargs):
        for i in xrange(retries):
            associations, dropped = self.associate(k_list, skip_probability, **kwargs)
            if not dropped:
                logging.info('Found after {} iterations\nSolution: {}'.format(i, associations))
                return associations, i
        logging.info('No solution found')

    def print_statistics(self):
        for fragment_id in xrange(len(self._fragments)):
            print '\nFragment {}'.format(fragment_id)
            for length, count in Counter(self._associations.get_group_size(fragment_id, group_id)
                                         for group_id in xrange(self._max_groups[fragment_id])).items():
                print '{} elements: {} groups'.format(length, count)
        print '\n{} ({:.3%}) lines dropped: {}'.format(len(self._dropped), float(len(self._dropped)) / len(self._table), self._dropped)


def main():
    from argparse import ArgumentParser
    from exporter import Exporter
    from tables import SqliteTable
    import json

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    parser = ArgumentParser(description='Create the loose-associations database.')
    parser.add_argument('config_file', help='JSON configuration file')
    args = parser.parse_args()

    with open(args.config_file) as config_file:
        config = json.load(config_file)

    table = SqliteTable(config['database'], config['table'])
    loose = Loose(table, config['constraints'], config['fragments'])
    associations, dropped = loose.associate(config['k_list'], config.get('skip_probability', 0))

    loose.print_statistics()
    Exporter(table, config['fragments'], associations).to_sqlite(config['output'])

if __name__ == '__main__':
    main()
