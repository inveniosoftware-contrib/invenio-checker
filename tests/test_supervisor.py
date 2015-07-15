import pytest
from frozendict import frozendict
import itertools

def w(*worker_names):
    ret = {}
    for name in worker_names:
        ret[name] = frozendict(all_workers[name.rstrip('`')])
    return frozenset([frozendict(((r1, r2),)) for r1, r2 in ret.items()])

all_workers = {
    'ab_ac_1_3':  frozendict({
        'allowed_paths': frozenset({'/a/b', '/a/c'}),
        'allowed_recids': frozenset({1, 2, 3}),
    }),
    'a_1_3':  frozendict({
        'allowed_paths': frozenset({'/a'}),
        'allowed_recids': frozenset({1, 2, 3}),
    }),
    'ad_1_3':  frozendict({
        'allowed_paths': frozenset({'/a/d'}),
        'allowed_recids': frozenset({1, 2, 3}),
    }),
    'ab_ac_4_6':  frozendict({
        'allowed_paths': frozenset({'/a/b', '/a/c'}),
        'allowed_recids': frozenset({4, 5, 6}),
    }),
    'ax_ay_1_3': frozendict({
        'allowed_paths': frozenset({'/a/x', '/a/y'}),
        'allowed_recids': frozenset({1, 2, 3}),
    }),
    'ab_1_3': frozendict({
        'allowed_paths': frozenset({'/a/b'}),
        'allowed_recids': frozenset({1, 2, 3}),
    }),
    'ab_4_6': frozendict({
        'allowed_paths': frozenset({'/a/b'}),
        'allowed_recids': frozenset({4, 5, 6}),
    }),
    'ab_6_12': frozendict({
        'allowed_paths': frozenset({'/a/b'}),
        'allowed_recids': frozenset({6, 7, 8, 9, 10, 11, 12}),
    }),
    'ab_1_3_20_70': frozendict({
        'allowed_paths': frozenset({'/a/b'}),
        'allowed_recids': frozenset({1, 2, 3, 20, 30, 40, 50, 60, 70}),
    }),
    'base': frozendict({
        'allowed_paths': frozenset(),
        'allowed_recids': frozenset({1, 2, 3}),
    }),
}

@pytest.mark.parametrize("_,worker_names,sets", [
    (
        "different_recids",
        ('ab_ac_1_3', 'ab_ac_4_6'),
        set(
            (
                w('ab_ac_1_3', 'ab_ac_4_6'),
            ),
        ),
    ),
    (
        "different_paths",
        ('ab_ac_1_3', 'ax_ay_1_3'),
        set(
            (
                w('ab_ac_1_3', 'ax_ay_1_3'),
            ),
        ),
    ),
    (
        "three_two_conflicting",
        ('ab_ac_1_3', 'ab_ac_1_3`', 'ad_1_3'),
        set(
            (
                w('ab_ac_1_3`', 'ad_1_3'),
                w('ab_ac_1_3'),  # FIXME: The ` and non-` are interchangeable
                                 # We could force this to be deterministic by
                                 # making one bigger.
            ),
        ),
    ),
    (
        "order",
        ('ab_4_6', 'ab_6_12', 'ab_1_3_20_70'),
        set(
            (
                w('ab_4_6', 'ab_1_3_20_70'),
                w('ab_6_12'),
            ),
        ),
    ),
    (
        "subpaths",
        ('a_1_3', 'ab_1_3'),
        set(
            (
                w('a_1_3'),
                w('ab_1_3'),
            ),
        ),
    ),
    (
        "base_subpaths",
        ('base', 'ab_1_3'),
        set(
            (
                w('base'),
                w('ab_1_3'),
            ),
        ),
    ),
    (
        "same",
        ('ab_ac_1_3', 'ab_ac_1_3`'),
        set(
            (
                w('ab_ac_1_3'),
                w('ab_ac_1_3`'),
            ),
        ),
    ),
])
def test_split_on_conflict(_, worker_names, sets):
    from invenio_checker.supervisor import split_on_conflict

    # Convert `sets`:
    #
    #     set(
    #         (
    #             w('ab_4_6', 'ab_1_3_20_70'),
    #             w('ab_6_12'),
    #         ),
    #     ),
    # to `expected_sets`:
    #     {{'ab_4_6', 'ab_1_3_20_70'}, {'ab_6_12'}}
    expected_sets = set()
    for superset in sets:
        expected_subset = set()
        for fdict in superset:
            expected_subset.update(fdict.keys())
        expected_sets.add(frozenset(expected_subset))
    expected_sets = frozenset(expected_sets)

    # Convert `worker_names`:
    #     ('ab_ac_1_3', 'ab_ac_1_3`')
    # to `workers`:
    #     {{'ab_ac_1_3': <worker_dict>, 'ab_ac_1_3`': <worker_dict>}}
    workers = {worker_name: all_workers[worker_name.rstrip('`')] for worker_name in worker_names}

    # Run
    output = split_on_conflict(workers)

    # Did we get the right sets?
    assert output == expected_sets
    # Are there any workers missing from the sets?
    assert len(workers) == len(tuple(itertools.chain.from_iterable(output)))
