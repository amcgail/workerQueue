from .job import Job
from abc import abstractmethod

class Serializer:

    @staticmethod
    @abstractmethod
    def encode(obj):
        pass

    @staticmethod
    @abstractmethod
    def decode(obj):
        pass