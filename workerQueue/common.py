from abc import ABC, abstractmethod
from datetime import datetime as dt
from logging import getLogger

from bson import ObjectId

logger = getLogger('worker')