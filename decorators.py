def memo(f):
    cache = {}

    def _f(*x):
        if x not in cache:
            cache[x] = f(*x)
        return cache[x]

    _f.cache = cache
    return _f
