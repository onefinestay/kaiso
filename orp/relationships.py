from orp.types import Persistable


class Relationship(Persistable):
    def __init__(self, start, end, **kwargs):
        super(Relationship, self).__init__(**kwargs)
        self.start = start
        self.end = end


class IsA(Relationship):
    pass


class InstanceOf(Relationship):
    pass
