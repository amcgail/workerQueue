from .common import *

from .job import Job
from .serializer import Serializer
class JobSerializer(Serializer):
    cls = Job

    @staticmethod
    def encode(c, obj):
        return {
            'id': obj.id
        }
    
    @staticmethod
    def decode(c, id):
        j = c.load(id)
        if not j.get('completed', False):
            return j
        
        return j['result']


class Context:
    def __init__(self, serializers=[]):
        self.db = None # must be set by the user
        self.jobs = None # must be set by the user

        self.serializers = serializers
        self.c2s = {s.cls: s for s in serializers}

    def add_serializer(self, *s):
        for _s in s:
            self.serializers.append(_s)
            self.c2s[_s.cls] = _s

    def load(self, _id):
        """
        This function is responsible for loading a job from the database.
        """
        classname = self.db.find_one({'_id': _id})['type']
        return getattr(self.jobs, classname)(_id=_id)
    

    def expand_args(self, a):
        """
        This function is responsible for expanding any references in the args to their actual values.
        That means instantiating any objects that are referred to by their ID.
        """

        if isinstance(a, dict) and 'cls' in a:

            target_cls = a['cls']
            args = dict(a)
            del args['cls']

            for s in self.serializers:
                if s.cls.__name__ == target_cls:
                    return s.decode(self, **args)

        elif isinstance(a, dict):
            return {
                k: self.expand_args(v)
                for k, v in a.items()
            }

        elif isinstance(a, list):
            return [self.expand_args(x) for x in a]

        return a

    def compress_args(self, a):
        """
        This function is responsible for compressing any references in the args to their ID.
        That means storing the ID of any objects that are referred to by their ID.
        """

        if hasattr(a, '__class__'):
            for s in self.serializers:
                if isinstance(a, s.cls):
                    return {
                        'cls': s.cls.__name__,
                        **s.encode(self, a)
                    }


        if isinstance(a, dict):
            return {
                k: self.compress_args(v)
                for k, v in a.items()
            }

        elif isinstance(a, list):
            return [self.compress_args(x) for x in a]

        return a

c = Context(serializers=[JobSerializer])