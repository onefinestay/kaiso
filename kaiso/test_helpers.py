from kaiso import types


class TemporaryStaticTypes(object):
    """ Testing helper to define temporary static types

    Types defined inside this context only live for the duration of a test,
    and so won't have have side effects leaking into other tests

    Usage:
        with TemporaryStaticTypes():
            class Foo(Entity):
                pass

    Though generally via the funcarg

        @pytest.fixture
        def foo(temporary_static_types):
            class Foo(Entity):
                pass
            return Foo

    or
        def test_foo(temporary_static_types):
            class Foo(Entity):
                pass

            assert(...)
    """

    def start(self):
        self.state = types.collected_static_classes.dump_state()

    def stop(self):
        types.collected_static_classes.load_state(self.state)

    def __enter__(self):
        self.start()

    def __exit__(self, *args, **kwargs):
        self.stop()
