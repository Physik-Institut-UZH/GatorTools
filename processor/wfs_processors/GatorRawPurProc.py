import numpy as np

from .GatorWfsProc import register_wfs_processor
from .GatorWfsProc import GatorWfsProc

@register_wfs_processor('rawpur')
class GatorRawPurProc(GatorWfsProc):
    def _post_init(self):
        pass
    #

    def doProc(self, wfs_bslnsubtr, df, raw_wfs):
        for wfname in self.chs_map:
            try:
                thr = float(self.chs_map[wfname]['processors']['rawpur']['threshold'])
            except KeyError:
                continue
            
            df[wfname + '_raw_pur'] = (df[wfname+'_raw_max_val']-df[wfname+'_raw_min_val'])>thr
        #

        return {}
    #

    def procSingleEvent(self, wfs_bslnsubtr:dict, raw_wfs:dict):
        return {}
    #