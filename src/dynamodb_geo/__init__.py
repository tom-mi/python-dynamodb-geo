from ._version import get_versions
from .table import GeoTable
from .configuration import GeoTableConfiguration
from .model import GeoPosition, QueryResult
from .enricher import GeoItemEnricher

__version__ = get_versions()['version']
del get_versions
