import numpy as np

from .GatorWfsProc import GatorWfsProc

class GatorRawWfsProc(GatorWfsProc):
    def _post_init(self):
        self.raw_wfs = dict() #This should be filled by the to doProc method only, but it will remain empty as theis class only produces basic wfs quantities in the dataframe
    #
    
    def doProc(self, wfs_bslnsubtr, df, raw_wfs):
        self.raw_wfs = raw_wfs
        #Quantities for the calculation of the wfs maxima
        for wf_name in self.chs_map:
            if raw_wfs[wf_name].ndim == 1:
                raw_wfs[wf_name] = raw_wfs[wf_name][None, :]
            #
            df[wf_name+'_raw_max_val'] = np.max(raw_wfs[wf_name], axis=1)
            df[wf_name+'_raw_max_pos'] = np.argmax(raw_wfs[wf_name], axis=1)
            df[wf_name+'_raw_min_val'] = np.min(raw_wfs[wf_name], axis=1)
            df[wf_name+'_raw_min_pos'] = np.argmin(raw_wfs[wf_name], axis=1)
        #
        return self.raw_wfs
    #

    def procSingleEvent(self, wfs_bslnsubtr:dict, raw_wfs:dict):
        return self.raw_wfs
    #