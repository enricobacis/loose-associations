from associations import Associations
from constraints import Constraints


class Loose(object):

    def __init__(self, table, constraints, fragments):
        self._table, self._tuples = table, len(table)
        self._fragments = fragments
        self._constraints = Constraints(constraints, fragments)

    def _get_group_data(self, fragment_id, group_id):
        return (self._table[row_id] for row_id in self._associations.get_group(fragment_id, group_id))

    def _get_groups(self, fragment_id, exclude_full=False):
        for group_id in xrange(self._max_groups[fragment_id]):
            group = self._associations.get_group(fragment_id, group_id)
            if not (exclude_full and len(group) >= self._k_list[fragment_id]):
                yield group_id

    def _are_groups_alike(self, group1, group2, fragment_id, constraint_id):
        return any(self._constraints.are_rows_alike_for(row1, row2, fragment_id, constraint_id)
                   for row1 in self._get_group_data(fragment_id, group1)
                   for row2 in self._get_group_data(fragment_id, group2))

    def _check_group_heterogenity(self, row, fragment_id, group_id):
        return not any(self._constraints.are_rows_alike(row, other_row, fragment_id)
                       for other_row in self._get_group_data(fragment_id, group_id))

    def _check_association_heterogenity(self, association, fragment_id, group_id):
        return not any(self._associations.exists(fragment_id, group_id, other_fragment, other_group)
                       for other_fragment, other_group in enumerate(association))

    def _check_deep_heterogenity(self, row, association, fragment_id, group_id):
        extended = association + [group_id]
        return all(all(all(any(not self._are_groups_alike(extended[frag_2], other_association[frag_2], frag_2, constraint_id)
                               for frag_2 in self._constraints.involved_fragments_for(constraint_id)
                               if frag_2 is not frag_1)
                           for other_association in self._associations.get_associated(frag_1, extended[frag_1]))
                       for frag_1 in self._constraints.involved_fragments_for(constraint_id))
                   for constraint_id in self._constraints.completed_with(fragment_id))

    def _check_heterogenity(self, row, association, fragment_id, group_id):
        return (self._check_group_heterogenity(row, fragment_id, group_id) and
                self._check_association_heterogenity(association, fragment_id, group_id) and
                self._check_deep_heterogenity(row, association, fragment_id, group_id))

    def _extend_association(self, row, association=[]):
        fragment_id = len(association)
        if (fragment_id == len(self._fragments)):
            return association

        for group_id in self._get_groups(fragment_id, True):
            if self._check_heterogenity(row, association, fragment_id, group_id):
                result = self._extend_association(row, association + [group_id])
                if result is not None:
                    return result

    def associate(self, k_list):
        self._k_list = k_list
        self._max_groups = [self._tuples / k for k in self._k_list]
        self._associations = Associations(len(self._fragments))
        self._dropped = []
        for row_id, row in enumerate(self._table):
            association = self._extend_association(row)
            if association is None:
                self._dropped.append(row_id)
            else:
                self._associations[row_id] = association
        return self._associations, self._dropped
