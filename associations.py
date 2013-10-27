from collections import defaultdict

class Associations(object):

    def __init__(self, columns):
        self._columns = columns
        self._data = defaultdict(list)
        self._indices = [defaultdict(set) for _ in xrange(columns)]

    def __delitem__(self, key):
        for i, v in enumerate(self._data[key]):
            self._indices[i][v].remove(key)
        del self._data[key]

    def __setitem__(self, key, val):
        if len(val) is not self._columns:
            raise ValueError("wrong value length")
        del self[key]
        self._data[key] = val
        for i, v in enumerate(val):
            self._indices[i][v].add(key)

    def __getitem__(self, key):
        return list(self._data[key])

    def __str__(self):
        return self._data.__str__()

    def get_group(self, fragment_id, group_id):
        return list(self._indices[fragment_id][group_id])

    def get_associated(self, fragment_id, group_id, select=slice(None)):
        return [self._data[key][select] for key in self._indices[fragment_id][group_id]]

    def exists(self, fragment1, group1, fragment2, group2):
        return group1 in self.get_associated(fragment2, group2, fragment1)
