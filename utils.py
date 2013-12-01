from itertools import chain, izip_longest
from random import shuffle, gauss

def average(lst):
    '''returns the average of the element in lst or zero if it is empty'''
    return 0 if not lst else float(sum(lst)) / len(lst)

def gaussint(*args, **kwargs):
    '''returns the int of a gauss call'''
    return int(gauss(*args, **kwargs))

def shuffled(lst):
    '''return a new list which is the shuffled version of lst'''
    new_list = list(lst)
    shuffle(new_list)
    return new_list

def memo(f):
    '''memoize decorator to cache values of a no-side-effect function'''
    cache = {}

    def _f(*x):
        if x not in cache:
            cache[x] = f(*x)
        return cache[x]

    _f.cache = cache
    return _f

def neighbours(n, lo, hi):
    '''returns a lazy iterable of neighbours of n in the range [lo, hi)'''
    return traverse(xrange(n - 1, lo - 1, -1), xrange(n + 1, hi))

def traverse(*iters):
    '''returns an iterator produced by the flatten traversal of the iterables'''
    return (x for x in chain.from_iterable(izip_longest(*iters)) if x is not None)

def placeholders(n):
    '''generate a placeholder with n fields for the DB-API executemany method'''
    return ', '.join(('?',) * n)
