import itertools as it
from frozendict import frozendict


def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = it.tee(iterable)
    next(b, None)
    return it.izip(a, b)


def _exclusive_paths(path1, path2):
    if not path1.endswith('/'):
        path1 += '/'
    if not path2.endswith('/'):
        path2 += '/'
    return path1.startswith(path2) or path2.startswith(path1)


def _touch_common_paths(paths1, paths2):
    assert paths1, 'Both workers must set some path.'
    assert paths2, 'Both workers must set some path.'
    for a, b in it.product(paths1, paths2):
        if _exclusive_paths(a, b):
            return True
    return False


def _touch_common_records(recids1, recids2):
    return recids1 & recids2


def _are_compatible(worker1, worker2):
    # Dear confused me, this should be a NAND.
    return not (_touch_common_records(worker1['allowed_recids'], worker2['allowed_recids']) and \
        _touch_common_paths(worker1['allowed_paths'], worker2['allowed_paths']))


def _frost_worker(worker, uuid):
    """Freeze workers and put UUID inside."""
    return frozendict({
        'allowed_paths': frozenset(worker['allowed_paths']),
        'allowed_recids': frozenset(worker['allowed_recids']),
        'uuid': uuid,
    })


def split_on_conflict(workers):
    workers = {_frost_worker(data, uuid) for uuid, data in workers.iteritems()}

    # Step 1) All possible compatible combinations
    acceptable_combinations = set()
    for comb in it.combinations(workers, 2):
        for worker1, worker2 in pairwise(comb):
            if _are_compatible(worker1, worker2):
                acceptable_combinations.add(frozenset((worker1, worker2)))

    # Step 2) All possible compatible combinations of length >=1
    acceptable_pairs = set()
    for ok1, ok2 in acceptable_combinations:
        if all(frozenset(combination) in acceptable_combinations
               for combination in it.combinations({ok1}|{ok2}, 2)):
            acceptable_pairs.add(frozenset({ok1}|{ok2}))
    # add workers which didn't match with anything (ones that are now single)
    for worker in workers:
        if not any(worker in acceptable_pair for acceptable_pair in acceptable_pairs):
            acceptable_pairs.add(frozenset({worker}))

    # Step 3) Supersets of length >=1 with no shared elements
    final_pairs = set()
    # For each superset, figure out which contained set already exists in
    # another superset and remove it from the current..
    # ..in reverse sum(id) size order beacause we want to start removing sets
    # from the biggest supersets. While this ordering is not guaranteed to make
    # a least pessimum distribution, it's fast to compute and should yield
    # good enough (R) results.
    gone_through = set()
    for cur_set in sorted(acceptable_pairs,
                          key=lambda s: sum((len(d['allowed_recids']) for d in s)),
                          reverse=True):
        f_exclude = set()
        for f in cur_set:
            # We remove `gone_through` because if we've been through something,
            # we either kept it, or deleted it because it existed somewhere
            # else.
            if {f} & \
            (
                    set(it.chain.from_iterable((acceptable_pairs - set([cur_set]))))
                    - set(it.chain.from_iterable(final_pairs))
                    - gone_through
            ):
                f_exclude.add(f)
        final_pairs.add(cur_set^f_exclude)
        for i in frozenset(it.chain(*set([cur_set]))):  # TODO
            gone_through.add(i)

    # Convert internal representation to uuid-value workers
    transformed_finals = set()
    for group in final_pairs:
        tr = set()
        for fdict in group:
            tr.add(
                frozendict(
                    {fdict['uuid']: frozendict({
                        'allowed_paths': fdict['allowed_paths'],
                        'allowed_recids': fdict['allowed_recids'],
                    })}
                )
            )
        transformed_finals.add(frozenset(tr))
    return set(transformed_finals)
