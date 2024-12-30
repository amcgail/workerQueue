import sys
from pathlib import Path
par = Path(__file__).resolve().parents[2]
sys.path.append(str(par))

from workerQueue import Worker, Serializer, Job, context
from logging import getLogger