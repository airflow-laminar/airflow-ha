__version__ = "1.0.0"

from .common import *
from .utils import *

try:
    from .operator import *
except ImportError:
    # Airflow is not installed,
    # don't raise an error for e.g.
    # airflow-supervisor client
    pass
