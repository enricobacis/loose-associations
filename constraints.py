from decorators import memo


class Constraints(object):

    def __init__(self, constraints, fragments):
        self._fragments = map(set, fragments)
        self._fragment_for = {attribute: fragment_id
                              for fragment_id, fragment in enumerate(self._fragments)
                              for attribute in fragment}

        # drop not relevant constraints
        all_attributes = set.union(*self._fragments)
        self._constraints = filter(lambda constraint: constraint <= all_attributes,
                                   map(set, constraints))

    @memo
    def involved_fragments_for(self, constraint_id):
        return set(map(self._fragment_for.get, self._constraints[constraint_id]))

    @memo
    def constraints_for(self, fragment_id):
        return filter(lambda c_id: fragment_id in self.involved_fragments_for(c_id),
                      xrange(len(self._constraints)))

    @memo
    def completed_with(self, fragment_id):
        return filter(lambda c_id: fragment_id == max(self.involved_fragments_for(c_id)),
                      xrange(len(self._constraints)))

    def are_rows_alike_for(self, row1, row2, fragment_id, constraint_id):
        return all(row1[attribute] == row2[attribute]
                   for attribute in (self._constraints[constraint_id] & self._fragments[fragment_id]))

    def are_rows_alike(self, row1, row2, fragment_id):
        return any(self.are_rows_alike_for(row1, row2, fragment_id, constraint_id)
                   for constraint_id in self.constraints_for(fragment_id))
