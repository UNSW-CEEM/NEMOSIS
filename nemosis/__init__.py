import logging
import sys
from .value_parser import _parse_datetime, _parse_column, _infer_column_data_types
from .data_fetch_methods import *

name = "osdan"

logging.getLogger(__name__).addHandler(logging.NullHandler())
logging.basicConfig(
    stream=sys.stdout, level=logging.INFO, format="%(levelname)s: %(message)s"
)
