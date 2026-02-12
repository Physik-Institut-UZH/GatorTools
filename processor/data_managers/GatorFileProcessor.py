from pathlib import Path

from .GatorRawFileHandler import GatorRawFileHandler
from ..wfs_processors import *

import numpy as np
import pandas as pd


class GatorFileProcessor:
    def __init__(self, fpath:str|Path, chs_map:GatorChsMap, keepwfs:bool=True):

        self.chs_lst = list(chs_map)
        self.chs_map = chs_map

        self.filehandler = GatorRawFileHandler(fpath=str(fpath), chs_lst=self.chs_lst)

        self.filehandler() #Load the data and for the moment keep the wfs

        self.df = self.filehandler.getDf()

        cols = list(self.df)

        self.df['filename'] = Path(fpath).name

        self.df = self.df[['filename']+cols]

        #Now the wfs can be released
        if(not keepwfs):
            self.filehandler.releaseWfs()
        #

        #This is the only waveforms processor that is not in the callbacks list. A Wf processor without this doesn't make sense.
        self.raw_wfs_proc = GatorRawWfsProc(chs_map=self.chs_map)
        self.bsln_corr_proc = GatorBslnSubtraction(chs_map=self.chs_map)

        self.callbacks = self._parseCallbacks()
    #

    def _parseCallbacks(self):
        cbnames_lst = list()
        for chname in self.chs_map:
            if not 'processors' in self.chs_map[chname]:
                continue
            #
            for cbname in self.chs_map[chname]['processors']:
                cbnames_lst.append(cbname)
            #
        #
        #Make the list with unique entries
        cbnames_lst = set(cbnames_lst)
        cb_lst = list()
        for cbname in cbnames_lst:
            cb_lst.append(get_wfs_proc_registry()[cbname](chs_map=self.chs_map))
        #
        return cb_lst
    #

    def __call__(self):
        raw_wfs = self.filehandler.getWfs()

        self.raw_wfs = raw_wfs.copy()
        
        #Compute minimal, basic quantities on raw waveforms and store them in the dataframe of each file
        self.raw_wfs_proc(
                wfs_bslnsubtr = None,
                df = self.df,
                raw_wfs = raw_wfs
                )
        
        #Execute the Baseline subtraction always as the second
        wfs_bslnsubtr = self.bsln_corr_proc(
                wfs_bslnsubtr = None,
                df = self.df,
                raw_wfs = raw_wfs
                )
        
        self.wfs_bslnsubtr = wfs_bslnsubtr.copy()
        
        #Sequential call of all the callbacks
        wfs_bslnsubtr = {ch_name: wfs_bslnsubtr[ch_name]['bslnsubtr'] for ch_name in wfs_bslnsubtr}
        for cb in self.callbacks:
            cb(
                wfs_bslnsubtr = wfs_bslnsubtr,
                df = self.df,
                raw_wfs = raw_wfs
                )
        #

        #Here all the quantities are inside the dataframe
        return self
    #

    def getDf(self):
        return self.df.copy()
    #