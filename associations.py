from collections import defaultdict
from utils import average


class Associations(defaultdict):

    def __init__(self, k_list):
        defaultdict.__init__(self, tuple)
        self._k_list  = k_list
        self._columns = len(self._k_list)
        self._indices = [defaultdict(set) for _ in xrange(self._columns)]
        self._fulls   = [set() for _ in xrange(self._columns)]

    def __setitem__(self, key, val):
        if val and len(val) != self._columns:
            raise ValueError('wrong value length for %s. Expected %i' % (val, self._columns))
        del self[key]
        defaultdict.__setitem__(self, key, tuple(val))
        for i, v in enumerate(val):
            group = self._indices[i][v]
            group.add(key)
            size = len(group)
            if size == self._k_list[i]:
                self._fulls[i].add(v)

    def __delitem__(self, key):
        if key in self:
            for i, v in enumerate(self[key]):
                group = self._indices[i][v]
                size = len(group)
                if size == self._k_list[i]:
                    self._fulls[i].remove(v)
                group.remove(key)
            defaultdict.__delitem__(self, key)

    def is_group_full(self, fragment_id, group_id):
        return group_id in self._fulls[fragment_id]

    def get_group(self, fragment_id, group_id):
        return self._indices[fragment_id][group_id].copy()

    def get_group_size(self, fragment_id, group_id):
        return len(self._indices[fragment_id][group_id])

    def get_groups(self, fragment_id):
        return self._indices[fragment_id]

    def get_average_group_size_in_fragment(self, fragment_id):
        return average(map(len, filter(None, self._indices[fragment_id].values())))

    def get_average_group_size(self):
        return average(map(self.get_average_group_size_in_fragment, xrange(self._columns)))

    def get_associated(self, fragment_id, group_id, select=slice(None)):
        return (self[key][select] for key in self._indices[fragment_id][group_id])

    def exists(self, fragment1, group1, fragment2, group2):
        return group1 in self.get_associated(fragment2, group2, fragment1)
