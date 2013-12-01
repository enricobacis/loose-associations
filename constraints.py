from utils import memo


class Constraints:

    def __init__(self, constraints, fragments):
        self._fragments = map(set, fragments)
        self._constraints = map(set, constraints)
        self._fragment_for = {attribute: fragment_id
                              for fragment_id, fragment in enumerate(self._fragments)
                              for attribute in fragment}

        # drop not relevant constraints
        all_attributes = set.union(set(), *self._fragments)
        self._constraints = [constraint for constraint in map(set, constraints)
                             if constraint <= all_attributes]

        self._max_fragment_for = [max(self.involved_fragments_for(c_id))
                                  for c_id in xrange(len(self._constraints))]

    @memo
    def involved_fragments_for(self, constraint_id):
        return set(self._fragment_for[a] for a in self._constraints[constraint_id])

    @memo
    def constraints_for(self, fragment_id):
        return [c_id for c_id in xrange(len(self._constraints))
                if fragment_id in self.involved_fragments_for(c_id)]

    @memo
    def completed_constraints_for(self, fragment_id, first_excluded_fragment_id):
        return [c_id for c_id in self.constraints_for(fragment_id)
                if self._max_fragment_for[c_id] < first_excluded_fragment_id]

    def are_rows_alike_for(self, row1, row2, fragment_id, constraint_id):
        return all(row1[attribute] == row2[attribute]
                   for attribute in (self._constraints[constraint_id] & self._fragments[fragment_id]))

    def are_rows_alike(self, row1, row2, fragment_id):
        return any(self.are_rows_alike_for(row1, row2, fragment_id, constraint_id)
                   for constraint_id in self.constraints_for(fragment_id))
