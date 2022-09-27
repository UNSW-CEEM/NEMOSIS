import logging
import sys
from . import data_fetch_methods
from .data_fetch_methods import *

name = "osdan"

logging.getLogger(__name__).addHandler(logging.NullHandler())
logging.basicConfig(
    stream=sys.stdout, level=logging.INFO, format="%(levelname)s: %(message)s"
)
