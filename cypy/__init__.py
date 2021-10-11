from .cli_utils import *
from .lmdb_utils import *
from .logging_utils import *
from .misc_utils import *
from .progress_utils import *
from .time_utils import *
from .metric_utils import *
from . import taiji

__version__ = '2021.10.11.a'

try:
    from .torch_utils import *
except ImportError as e:
    print('Not found torch. Some funcs may not work.')
    print(str(e))

try:
    from .lr_utils import *
except ImportError as e:
    print('Not found torch. Some funcs may not work.')
    print(str(e))

try:
    from .setup_utlis import *
except ImportError as e:
    print('Not found torch. Some funcs may not work.')
    print(str(e))