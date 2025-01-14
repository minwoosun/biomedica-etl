# read version from installed package
from importlib.metadata import version
from .memory_usage      import get_memory_usage
__version__ = version("pmc_oa")
