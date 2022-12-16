"""
pydatatask is a library for building data pipelines. Sounds familiar? The cool part here is that you are not restricted
in the way your data is stored or the way your tasks are executed.

A **task** is one phase of computation.
It is parameterized (instantiated) by a single **job** that it is currently working on.
A **pipeline** is a collection of tasks.
Tasks read and write data from **repositories**, which are arbitrary key-value stores.
"""
from .main import *
from .pipeline import *
from .pod_manager import *
from .proc_manager import *
from .repository import *
from .resource_manager import *
from .session import *
from .task import *

__version__ = "0.1.5-dev.1"
released_version = "0.1.4"
