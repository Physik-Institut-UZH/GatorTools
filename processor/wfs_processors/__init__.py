import pkgutil
import importlib

from . import GatorWfsProc as GatorWfsProcModule
from .GatorWfsProc import GatorChsMap
from .GatorRawWfsProc import GatorRawWfsProc
from .GatorBslnSubtraction import GatorBslnSubtraction


from typing import Dict, Type

# This loop below should import only the derived classes of GatorWfsProcModule.GatorWfsProc base class. 
# They will self register via the register_wfs_processor decorator
for _, module_name, _ in pkgutil.iter_modules(__path__):
    importlib.import_module(f"{__name__}.{module_name}")
#

def get_wfs_proc_registry():
    return GatorWfsProcModule._WFS_PROC_REGISTRY
#

__all__ = [
    "GatorChsMap",
    "GatorRawWfsProc",
    "GatorBslnSubtraction",
    "get_wfs_proc_registry",
]