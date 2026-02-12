from os import path
import pandas as pd
import numpy as np


from .GatorDatasetsStorage import GatorDatasetsStorage
from ..wfs_processors import *

class GatorDatasetsProcessor(GatorDatasetsStorage):
    #The aim of this class is to process all the waveforms file by file without keeping the waveforms in memory and to create a dataframe of the extracted quantities.
    def __init__(self, datadir:str, datasets:list|str, chs_map:GatorChsMap):
        super().__init__(datadir=datadir,
                         datasets=datasets,
                         chs_map=chs_map,
                         keepwfs=False
                        )
        
        #This is the only waveforms processor that is not in the callbacks list. A Wf processor without this doesn't make sense.
        self.bsln_corr_proc = GatorBslnSubtraction(chs_map=self.chs_map, processor=None)

        self.callbacks = list()
    #

    def setCallbackList(self, callbacks:list):
        for cb in callbacks:
            self.addCallBack(cb.setProcessor(self))
        #
    #

    def addCallback(self, cb):
        self.callbacks.append(cb)
    #    

    def __call__(self):
        #The first step is to compute the baselines and build the the processed waveforms per file handler
        for iFile, filehand in enumerate(self.files_handl_lst):
            print(f'Processing file: {filehand}')
            raw_wfs = filehand.getWfs()
            df = self.dfs_lst[iFile]
            
            #Compute minimal, basic quantities on raw waveforms and store them in the dataframe of each file
            self.ComputeWfsRawQuantities(raw_wfs=raw_wfs, df=df)

            wfs_bslncorr = self.bsln_corr_proc(wfs_bslncorr=None, df=df, raw_wfs=raw_wfs)
            
            #Sequential call of all the callbacks
            wfs_bslncorr = {ch_name: wfs_bslncorr[ch_name]['bslnsubtr'] for ch_name in wfs_bslncorr}
            for cb in self.callbacks:
                cb(wfs_bslncorr=wfs_bslncorr, df=df, raw_wfs=raw_wfs)
            #
        return self
    #

    def ComputeWfsRawQuantities(self, raw_wfs:dict, df:pd.DataFrame):
        
        for wf_name in self.chs_lst:
            if raw_wfs[wf_name].ndim == 1:
                raw_wfs[wf_name] = raw_wfs[wf_name][None, :]
            #
            df[wf_name+'_raw_max_val'] = np.max(raw_wfs[wf_name], axis=1)
            df[wf_name+'_raw_max_pos'] = np.argmax(raw_wfs[wf_name], axis=1)
            df[wf_name+'_raw_min_val'] = np.min(raw_wfs[wf_name], axis=1)
            df[wf_name+'_raw_min_pos'] = np.argmin(raw_wfs[wf_name], axis=1)
        #
    #

    def BslnCorrProc(self, raw_wfs, df):
        wfs_bslncorr = dict()

        #Quantities for the calculation of the wfs maxima
        wf_n_samps = self.raw_wfs.shape[1]

        for wf_name in self.raw_wfs:
            means = np.mean(raw_wfs[wf_name][:, :self.bslnsamps], axis=1)
            df[wf_name+'_bslns_mean'] = means
            df[wf_name+'_bslns_rms'] = np.std(raw_wfs[wf_name][:, :self.bslnsamps], axis=1)
            medians = np.median(raw_wfs[wf_name][:, :self.bslnsamps], axis=1)
            df[wf_name+'_bslns_med'] = medians
            df[wf_name+'_bslns_mad'] = np.median(np.abs(raw_wfs[wf_name][:, :self.bslnsamps] - medians[:, np.newaxis]), axis=1)

            # Make the wfs with corrected bslns
            if self.bslns_meth=='mean':
                bslns = means
            elif self.bslns_meth=='median':
                bslns = medians
            else:
                raise RuntimeError('Unexpected method for the baselines calculation ({}). The only inplemented methods are "mean" and "median".')
            #
            
            wfs_corr = raw_wfs - bslns
            
            if self.neg_pulse:
                wfs_corr = -1.0*wfs_corr
            #
            wfs_bslncorr[wf_name] = wfs_corr
            
            # Calculate the maxima of each wf
            samp_max_arr = np.argmax(wfs_corr, axis=1)
            df[wf_name+'_samp_max'] = samp_max_arr

            wfs_max_lst = []

            for iWf, samp_max in enumerate(samp_max_arr):
                if ((samp_max - 2) >= 0) and ((samp_max + 2) < wf_n_samps):
                    wfs_max_lst.append(np.mean(wfs_corr[wf_name][iWf, samp_max-2:samp_max+3]))
                else:
                    wfs_max_lst.append(wfs_corr[wf_name][iWf, samp_max])
                #
            #
            df[wf_name+'_ampl_max'] = wfs_max_lst
        #
        return wfs_bslncorr
    #
#