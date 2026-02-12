import numpy as np

from .GatorWfsProc import register_wfs_processor
from .GatorWfsProc import GatorWfsProc
from ..wfs_utils import trapezoidalFilt

@register_wfs_processor('trapezoid')
class TrapezoidProc(GatorWfsProc):
    def _post_init(self):
        self.trap_filters = dict()
    #

    def doProc(self, wfs_bslnsubtr, df, raw_wfs=None):
        self.trap_filters = dict()
        
        for wf_name in self.chs_map:
            try:
                shape_time = self.chs_map[wf_name]['processors']['trapezoid']['shape_time']
                tau = self.chs_map[wf_name]['processors']['trapezoid']['tau']
                flat_top = self.chs_map[wf_name]['processors']['trapezoid']['flat_top']
                wfs = wfs_bslnsubtr[wf_name]
            except KeyError as err:
                continue
            #
            
            trap_filter = trapezoidalFilt(wfs, shape_time, tau, flat_top)
            

            #Compute the energy and put it in the dataframe
            df[wf_name+'_energy_trap'] = np.max(trap_filter, axis=1)

            df[wf_name+'_trap_pur'] = np.sum(trap_filter, axis=1)/np.max(trap_filter, axis=1) #Equivalent length: trapezoidal area/trapezoidal max

            self.trap_filters[wf_name] = {'trapezoid':trap_filter}
        #
        
        return self.trap_filters
    #

    def procSingleEvent(self, wfs_bslnsubtr:dict, raw_wfs:dict):
        trap_filters = dict()

        for wf_name in self.chs_map:
            try:
                shape_time = self.chs_map[wf_name]['processors']['trapezoid']['shape_time']
                tau = self.chs_map[wf_name]['processors']['trapezoid']['tau']
                flat_top = self.chs_map[wf_name]['processors']['trapezoid']['flat_top']
                _wf = wfs_bslnsubtr[wf_name].copy()
            except KeyError:
                continue
            #
            
            
            if ((_wf.ndim==2) and (_wf.shape[0]>1)) or (_wf.ndim>2):
                raise TypeError(f'The "raw_wf" must be a Numpy array corresponding to a single waveform, while it is an array of shape {_wf.shape}.')
            #

            #Do not modify the original array
            _wf = _wf.flatten()

            trap_filters[wf_name] = {'trapezoid':trapezoidalFilt(_wf, shape_time, tau, flat_top)}
        #
        return trap_filters
    #
#