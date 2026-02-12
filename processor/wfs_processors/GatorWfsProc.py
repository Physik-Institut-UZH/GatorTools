import json
from pathlib import Path

from processor import data_managers

from typing import TYPE_CHECKING
from typing import (Dict, Type)

if TYPE_CHECKING:
    from ..data_managers import GatorDatasetsProcessor

_WFS_PROC_REGISTRY: Dict[str, Type] = {}

def register_wfs_processor(name: str):
    def decorator(cls):
        if name in _WFS_PROC_REGISTRY:
            raise RuntimeError(f"Duplicate WFS processor name: {name}")

        _WFS_PROC_REGISTRY[name] = cls
        return cls
    return decorator
#

class GatorChsMap:
    def __init__(self, *, jsonfile=None, jsonstr=None):
        """
        Initialize GatorChsMap from a JSON file or a JSON-like string.
        Priority: jsonstr > jsonfile
        """

        if jsonstr:
            json_fixed = jsonstr.replace("'", '"')
            self.chs_map = json.loads(json_fixed)
        elif jsonfile:
            path = Path(jsonfile)
            if not path.exists():
                raise FileNotFoundError(f"JSON file not found: {path}")
            with open(path, "r") as f:
                self.chs_map = json.load(f)
        else:
            raise ValueError('Either the "jsonfile" or "jsonstr" arguments must be provided.')
        #
        self.chs_lst = list(self.chs_map)
    #

    def __repr__(self):
        return json.dumps(self.chs_map, indent=4)
    #

    def __getitem__(self, key):
        return self.chs_map[key]
    #

    def __iter__(self):
        # list(obj) will iterate over self.chs_lst
        return iter(self.chs_lst)
    #

    def getChsMap(self):
        return self.chs_map
    #

    def getChsLst(self):
        return self.chs_lst
    #
#


class GatorWfsProc:
    #Generic base class for the data processors callbacks
    def __init__(self, chs_map:GatorChsMap, dataprocessor=None):
        self.chs_map = chs_map
        self.dataprocessor = dataprocessor
        if self.dataprocessor is not None:
            self.dataprocessor.addCallback(self)
        #
        self._post_init()  # Force subclasses to define this
    #

    def _post_init(self):
        """Subclasses must implement this method."""
        if str(self.__class__.__name__)=="GatorWfsProc":
            raise NotImplementedError('The "GatorWfsProc" class is meant to provide only the interface and shall not be instanced.')
        else:
            raise NotImplementedError(f"{self.__class__.__name__} must implement _post_init()")
        #
    #

    def setProcessor(self, dataprocessor):
        self.dataprocessor = dataprocessor
        return self
    #

    def __call__(self, wfs_bslnsubtr, df, raw_wfs=None):
        print(f'{str(self.__class__.__name__)}: start processing.')
        return self.doProc(wfs_bslnsubtr, df, raw_wfs)
    #

    def doProc(self, wfs_bslnsubtr, df, raw_wfs):
        """Subclasses must implement this method."""
        if str(self.__class__.__name__)=="GatorWfsProc":
            raise NotImplementedError('The "GatorWfsProc" class is meant to provide only the interface and shall not be instanced.')
        else:
            raise NotImplementedError(f"{self.__class__.__name__} must implement _post_init()")
        #
    #

    def procSingleEvent(self, wfs_bslnsubtr:dict, raw_wfs:dict):
        """
        This method does exaclty what the 'doProc' does, but it doesn't use (and modify) any dataframe.
        It only returns the waveforms of the channels that require the action of the specific processor. 
        It is meant to be used with data visualization tools.
        Subclasses only must implement this method.
        """

        if str(self.__class__.__name__)=="GatorWfsProc":
            raise NotImplementedError('The "GatorWfsProc" class is meant to provide only the interface and shall not be instanced.')
        else:
            raise NotImplementedError(f"{self.__class__.__name__} must implement _post_init()")
        #
    #




