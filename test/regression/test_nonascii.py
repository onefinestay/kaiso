# coding: utf-8


def test_nonascii(manager):
    assert manager.query(
        "START n=node(*) WHERE n.foo = {foo} RETURN n",
        foo=u'föö',
    )
