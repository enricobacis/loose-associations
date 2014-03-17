from itertools import izip_longest, product, chain
from random import shuffle, gauss
from time import time

def half_divisors(n):
    '''returns the divisors of n up to sqrt(n)'''
    return [x for x in xrange(1, int((n ** .5) + 1)) if not n % x]

def divisors(n):
    '''returns the divisors of n'''
    return sorted(set(x for div in half_divisors(n) for x in (div, n / div)))

def min_product(lst):
    '''returns the product of the two minimum elements in a list'''
    slst = sorted(lst)
    return slst[0] * slst[1]

def k_lists(k, frag_num):
    '''returns all the k_list enforcing a privacy k over frag_num fragments'''
    return [[lo if i == j else (k / lo) for j in xrange(frag_num)]
            for lo in half_divisors(k) for hi in [k / lo]
            for i in xrange(1 if lo == hi else frag_num)]

def score(lst):
    '''returns a score for a lst (lowest is better)'''
    return sum(abs(a - b) for i, a in enumerate(lst) for b in lst[i + 1:])

def best_k_lists(*args):
    '''returns the k_lists with minimum score'''
    lists = k_lists(*args)
    scores = map(score, lists)
    lowest_score = min(scores)
    return [lst for i, lst in enumerate(lists) if scores[i] == lowest_score]

def average(lst):
    '''returns the average of the element in lst or zero if it is empty'''
    return 0 if not lst else float(sum(lst)) / len(lst)

def gaussint(*args, **kwargs):
    '''returns the int of a gauss call'''
    return int(gauss(*args, **kwargs))

def shuffled(lst):
    '''returns a new list which is the shuffled version of lst'''
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
    return traverse(xrange(n + 1, hi), xrange(n - 1, lo - 1, -1))

def traverse(*iters):
    '''returns an iterator produced by the flatten traversal of the iterables'''
    return (x for x in chain.from_iterable(izip_longest(*iters)) if x is not None)

def placeholders(n):
    '''generate a placeholder with n fields for the DB-API executemany method'''
    return ', '.join(('?',) * n)

def make_withtime():
    '''return a function prepending the time passed from now to a string'''
    started = time()
    return lambda string: '[{:.2f}s] {}'.format(time() - started, string)
