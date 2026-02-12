from os import path
from glob import glob

import numpy as np
import pandas as pd

from .GatorRawFileHandler import GatorRawFileHandler
from ..wfs_processors import GatorChsMap

class GatorDatasetsStorage:
    def __init__(self, chs_map:GatorChsMap, datadir:str, datasets:list|str, keepwfs=False):

        if isinstance(chs_map, GatorChsMap):
            self.chs_map = chs_map
        else:
            raise TypeError(f'GatorDatasetsStorage.__init__: The "chs_map" argument must be of "GatorChsMap" type, while it was given of "{type(chs_map)}" type.')
        #

        self.datadir = datadir

        if isinstance(datasets, list):
            self.datasets = datasets
        elif isinstance(datasets, str):
            self.datasets = [datasets]
        else:
            raise TypeError('GatorDatasetsStorage.__init__: The "datasets" argument can only be a string or a list of strings.')
        #

        self.chs_lst = list(chs_map)
        
        self.files_handl_lst = list()
        self.dfs_lst = list()
        
        for _ids, ds in enumerate(self.datasets):
            _paths_lst = glob(path.join(self.datadir, ds, '*.root'))
            for _iFile, _fpath in enumerate(_paths_lst):
                _filehandler = GatorRawFileHandler(fpath=_fpath, chs_lst=self.chs_lst)
                _filehandler() #Load the data and for the moment keep the wfs
                _df = _filehandler.getDf()
                cols = list(_df)
                _df['dataset'] = ds
                _df['datasetId'] = _ids
                _df['filename'] = path.basename(_fpath)
                _df['fileId'] = _iFile
                self.files_handl_lst.append(_filehandler)
                _df['wfId'] = _df.index
                
                _df = _df[['filename', 'fileId', 'wfId']+cols]
                
                self.dfs_lst.append(_df)

                #Now the wfs can be released
                if(not keepwfs):
                    _filehandler.releaseWfs()
                #
            #
        #
    #

    def getMergedDf(self):
        return pd.concat(self.dfs_lst, ignore_index=True)
    #

    def getSelWf(self, fileId:int, wfId:int):
        _wfs_dict = self.files_handl_lst[fileId].getWfs()
        return {ch_name: wf[wfId] for ch_name, wf in _wfs_dict.items()}
    #

    def getChsMap(self):
        return self.chs_lst
#