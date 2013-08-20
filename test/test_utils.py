from kaiso.utils import dict_difference


def test_dict_difference():

    a = {1: 2, 3: 4}
    b = {3: 4, 5: 6}

    assert dict_difference(a, b) == {1: 2}
    assert dict_difference(b, a) == {5: 6}

    assert dict_difference(a, {}) == a
    assert dict_difference({}, a) == {}

    # non-hashable value
    c = {7: set()}

    assert dict_difference(a, c) == a

    # same keys
    d = {1: 2}
    e = {1: "2"}

    assert dict_difference(d, e) == d
