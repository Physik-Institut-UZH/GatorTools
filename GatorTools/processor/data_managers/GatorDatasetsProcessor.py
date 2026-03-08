from os import path
import pandas as pd
import numpy as np


from .GatorDatasetsStorage import GatorDatasetsStorage
from ..wfs_processors import *

#TODO: This function is also used by the GatorFileProcessor class as a method. Move it to the wfs_processor base module to be shared by all kind of data managers.
def parse_callbacks(chs_map:GatorChsMap) -> list:
    cbnames_lst = list()
    for chname in chs_map:
        if not 'processors' in chs_map[chname]:
            continue
        #
        for cbname in chs_map[chname]['processors']:
            cbnames_lst.append(cbname)
        #
    #
    #Make the list with unique entries
    cbnames_lst = set(cbnames_lst)
    cb_lst = list()
    for cbname in cbnames_lst:
        cb_lst.append(get_wfs_proc_registry()[cbname](chs_map=chs_map))
    #
    return cb_lst
#

class GatorDatasetsProcessor(GatorDatasetsStorage):
    #The aim of this class is to process all the waveforms file by file without keeping the waveforms in memory and to create a dataframe of the extracted quantities.
    def __init__(self, datadir:str, datasets:list|str, chs_map:GatorChsMap):
        super().__init__(datadir=datadir,
                         datasets=datasets,
                         chs_map=chs_map,
                         keepwfs=False
                        )
        
        #These are the only waveforms processor that is not in the callbacks list. A Wf processor without this doesn't make sense.
        self.raw_wfs_proc = GatorRawWfsProc(chs_map=self.chs_map)
        self.bsln_corr_proc = GatorBslnSubtraction(chs_map=self.chs_map)

        self.callbacks = parse_callbacks(chs_map)
    #

    def __call__(self):
        #The first step is to compute the baselines and build the the processed waveforms per file handler
        for iFile, filehand in enumerate(self.files_handl_lst):
            print(f'Processing file: {filehand}')
            raw_wfs = filehand.getWfs()
            df = self.dfs_lst[iFile]
            
            #Compute minimal, basic quantities on raw waveforms and store them in the dataframe of each file
            self.raw_wfs_proc(
                    wfs_bslnsubtr = None,
                    df = df,
                    raw_wfs = raw_wfs
                    )

            #Execute the Baseline subtraction always as the second
            wfs_bslnsubtr = self.bsln_corr_proc(
                    wfs_bslnsubtr = None,
                    df = df,
                    raw_wfs = raw_wfs
                    )

            
            #Sequential call of all the callbacks
            wfs_bslnsubtr = {ch_name: wfs_bslnsubtr[ch_name]['bslnsubtr'] for ch_name in wfs_bslnsubtr}
            for cb in self.callbacks:
                cb(
                    wfs_bslnsubtr = wfs_bslnsubtr,
                    df = df,
                    raw_wfs = raw_wfs
                )
            #
        return self
    #
#