from associations import Associations
from constraints import Constraints
from collections import defaultdict, Counter, deque
from itertools import count, chain
from operator import itemgetter
from random import random
from utils import neighbours, make_withtime
import logging


class Loose:

    def __init__(self, table, constraints, fragments):
        self._table, self.tuples = table, len(table)
        self._fragments = map(table.to_indices, fragments)
        self._constraints = Constraints(map(table.to_indices, constraints), self._fragments)

    def _get_group_data(self, fragment_id, group_id):
        return ((row_id, self._table[row_id]) for row_id in self._associations.get_group(fragment_id, group_id))

    def _get_nonfull_groups(self, fragment_id, allow_skips=True, **kwargs):
        groups_to_check = xrange(self._first_nonfull[fragment_id], self._last_usable[fragment_id] + 1)
        return (group_id for group_id in groups_to_check
                if not self._associations.is_group_full(fragment_id, group_id))
                # uncomment this to allow skips
                # if not allow_skips or not self._skip_probability or random() > self._skip_probability)

    def _are_groups_alike(self, current_group, other_group, fragment_id, constraint_id, current_row):
        current_rows = self._get_group_data(fragment_id, current_group)
        if current_row:
            current_rows = chain([(0, current_row)], current_rows)

        for _, row1 in current_rows:
            for _, row2 in self._get_group_data(fragment_id, other_group):
                if self._constraints.are_rows_alike_for(row1, row2, fragment_id, constraint_id):
                    return True
        return False

    def _check_group_heterogenity(self, row, fragment_id, group_id):
        for _, other_row in self._get_group_data(fragment_id, group_id):
            if self._constraints.are_rows_alike(row, other_row, fragment_id):
                # logging.trace('GROUP VIOLATED fragment {} group {}'.format(fragment_id, group_id))
                return False
        return True

    def _check_association_heterogenity(self, association, fragment_id, group_id):
        for other_fragment, other_group in enumerate(association):
            if other_fragment != fragment_id:
                if self._associations.exists(fragment_id, group_id, other_fragment, other_group):
                    # logging.trace('ASSOCIATION VIOLATED fragment {} group {}'.format(fragment_id, group_id))
                    return False
        return True

    def _check_deep_heterogenity(self, row, association, fragment_id, group_id):
        return (self._check_deep_heterogenity_of_association(row, association, fragment_id, group_id) and
                self._check_deep_heterogenity_of_associated_groups(row, fragment_id, group_id))

    def _check_deep_heterogenity_of_association(self, row, association, fragment_id, group_id):
        for constraint_id in self._constraints.constraints_for(fragment_id):
            for fragment1 in self._constraints.involved_fragments_for(constraint_id):
                group1 = group_id if (fragment1 == fragment_id) else -1 if fragment1 >= len(association) else association[fragment1]
                for other_association in self._associations.get_associated(fragment1, group1):
                    if other_association != association:
                        for fragment2 in self._constraints.involved_fragments_for(constraint_id):
                            if fragment2 != fragment1:
                                group2 = group_id if (fragment2 == fragment_id) else -1 if fragment2 >= len(association) else association[fragment2]
                                if not self._are_groups_alike(group2, other_association[fragment2], fragment2, constraint_id, row):
                                    break
                        else:
                            # logging.trace("DEEP ASSOCIATION VIOLATED fragment {} group {}".format(fragment_id, group_id))
                            return False
        return True

    def _check_deep_heterogenity_of_associated_groups(self, row, fragment_id, group_id):
        constraints_for = set(self._constraints.constraints_for(fragment_id))
        # take all the pre-existing associations in (fragment_id, group_id)
        for association1 in self._associations.get_associated(fragment_id, group_id):
            # for every fragment1
            for fragment1 in xrange(len(self._fragments)):
                # which is not fragment_id
                if fragment1 != fragment_id:
                    # I already know which constraints can break
                    # only the ones that insists on both fragment_id and fragment1
                    constraints_to_check = constraints_for & set(self._constraints.constraints_for(fragment1))
                    # take another association, associated with (fragment1 , association1[fragment1])
                    for association2 in self._associations.get_associated(fragment1, association1[fragment1]):
                        # which is not association1
                        if association1 != association2:
                            # for every constraint that can break
                            for constraint_id in constraints_to_check:
                                # there must exists a fragment2 (insisting on the constraint)
                                for fragment2 in self._constraints.involved_fragments_for(constraint_id):
                                    # which is not fragment2
                                    if fragment2 != fragment1:
                                        # fow which the constraint holds
                                        if not self._are_groups_alike(association1[fragment2], association2[fragment2],
                                            # if fragment2 is the same as fragment_id, we inform the method are_groups_alike
                                            # about the fact that we want to insert the tuple in that fragment
                                            fragment2, constraint_id, row if (fragment2 == fragment_id) else None):
                                            break
                                else:
                                    # logging.trace("INNER {} {}".format(association1, association2))
                                    return False
        return True

    # fragment_id is needed because it's not always bound to the association length. It is in extend_association
    # but not in redistribute_group. There we want to check just a single fragment from an already complete association.
    def _check_heterogenity(self, row, association, fragment_id, group_id):
        return (self._check_group_heterogenity(row, fragment_id, group_id) and
                self._check_association_heterogenity(association, fragment_id, group_id) and
                self._check_deep_heterogenity(row, association, fragment_id, group_id))

    def _extend_association(self, row, row_id, association=[]):
        fragment_id = len(association)
        if (fragment_id == len(self._fragments)):
            return association

        for group_id in self._get_nonfull_groups(fragment_id, row_id=row_id):
            if self._check_heterogenity(row, association, fragment_id, group_id):
                result = self._extend_association(row, row_id, association + [group_id])
                if result is not None:
                    return result

    def _full_neighbours_groups(self, fragment_id, group_id, step=1):
        return (new_group_id for new_group_id in neighbours(group_id, 0, self._last_usable[fragment_id] + 1)
                if self._associations.is_group_full(fragment_id, new_group_id))
        
    def _redistribute_group(self, fragment_id, group_id, step=1):
        logging.debug('redistributing group {} in fragment {}'.format(group_id, fragment_id))
        for row_id, row in self._get_group_data(fragment_id, group_id):
            association = self._associations[row_id]
            if association:
                for new_group_id in self._full_neighbours_groups(fragment_id, group_id, step):
                    if self._check_heterogenity(row, association, fragment_id, new_group_id):
                        logging.debug('fragment {} row {}: group {} -> group {}'.format(fragment_id, row_id, group_id, new_group_id))
                        new_association = list(association)
                        new_association[fragment_id] = new_group_id
                        self._associations[row_id] = new_association
                        break
                else:
                    logging.debug('fragment {} row {} : group {} -> not reallocable'.format(fragment_id, row_id, group_id))
                    self._opstack.append((1, (row_id, step)))    # delete

    def _delete_row(self, row_id, step=1):
        self._dropped.add(row_id)
        association = self._associations[row_id]
        logging.debug('deleting row {} = {}'.format(row_id, association))
        del self._associations[row_id]
        for fragment_id, group_id in enumerate(association):
            if not self._associations.is_group_full(fragment_id, group_id):
                self._opstack.append((0, (fragment_id, group_id, step + 1)))    # redistribute
        logging.info('row {} deleted (step {})'.format(row_id, step))

    def _update_first_nonfull(self, association):
        for fragment_id, group_id in enumerate(association):
            if group_id == self._first_nonfull[fragment_id]:
                while (self._associations.is_group_full(fragment_id, self._first_nonfull[fragment_id])):
                    self._first_nonfull[fragment_id] += 1

    def _update_last_usable(self, association):
        for fragment_id, group_id in enumerate(association):
            if group_id >= self._last_usable[fragment_id]:
                self._last_usable[fragment_id] = group_id + 1

    def _update_pointers(self, association):
        self._update_first_nonfull(association)
        self._update_last_usable(association)

    def associate(self, k_list, skip_probability=0, **kwargs):
        assert(len(k_list) == len(self._fragments))
        print_stats_every = kwargs.get('print_stats_every', 1000)

        self._k_list = k_list
        self._first_nonfull = [0 for _ in xrange(len(self._fragments))]
        self._last_usable = [0 for _ in xrange(len(self._fragments))]
        self._associations = Associations(self._k_list)
        self._dropped = set()
        self._skip_probability = skip_probability

        withtime = make_withtime()
        for row_id, row in enumerate(self._table):
            if row_id and not row_id % print_stats_every:
                logging.info(withtime('associating row: %i firsts: %s lasts: %s'
                    % (row_id, self._first_nonfull, self._last_usable)))

            association = self._extend_association(row, row_id)
            if association is None:
                self._dropped.add(row_id)
                logging.info('row_id {} dropped at first scan'.format(row_id))
            else:
                self._associations[row_id] = association
                self._update_pointers(association)

        self._opstack = deque()
        for fragment_id in xrange(len(self._fragments)):
            for group_id in self._get_nonfull_groups(fragment_id, False):
                if self._associations.get_group_size(fragment_id, group_id):
                    self._opstack.append((0, (fragment_id, group_id)))    # redistribute

        counter = count()
        while self._opstack:
            if not next(counter) % 10:
                logging.info(withtime('opstack length: %i' % len(self._opstack)))
            operation = self._opstack.pop()
            fn = self._delete_row if operation[0] else self._redistribute_group
            fn(*operation[1])

        logging.info(withtime('done after %i stack operations' % (next(counter))))
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
            for length, count in Counter(group_size for group_id in xrange(self._last_usable[fragment_id])
                                         for group_size in (self._associations.get_group_size(fragment_id, group_id),)
                                         if group_size is not 0).items():
                print '{} elements: {} groups'.format(length, count)
        print '\n{} ({:.3%}) lines dropped: {}'.format(len(self._dropped), float(len(self._dropped)) / self.tuples, self._dropped)


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
