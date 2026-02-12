import numpy as np

from .GatorWfsProc import register_wfs_processor
from .GatorWfsProc import GatorWfsProc
from ..wfs_utils import (
    gaussian_filter,
    find_rel_maxima,
)

@register_wfs_processor('gaussfilter')
class WfsGaussianFilters(GatorWfsProc):
    def _post_init(self):
        self.wfs_smooth = dict()
    #

    def doProc(self, wfs_bslnsubtr, df, raw_wfs=None):
        self.wfs_smooth = dict()
        
        for wf_name in self.chs_map:
            
            try:
                sigma = self.chs_map[wf_name]['processors']['gaussfilter']['sigma']
                kernel_half_width = self.chs_map[wf_name]['processors']['gaussfilter']['kernel_half_width']
                wfs = wfs_bslnsubtr[wf_name]
            except KeyError:
                continue
            #

            wfs_smooth = gaussian_filter(wfs, sigma, kernel_half_width, derivative=False)
            self.wfs_smooth[wf_name] = {'gaussfilter':{'swf':wfs_smooth}}

            df[wf_name+'_smooth_pulse_ampl'] = np.max(wfs_smooth, axis=1)
            df[wf_name+'_smooth_pulse_maxpos'] = np.argmax(wfs_smooth, axis=1)

            if ('find_pulses' in self.chs_map[wf_name]['processors']['gaussfilter']) and (self.chs_map[wf_name]['processors']['gaussfilter']['find_pulses']==True):
                ampl_min_thr = self.chs_map[wf_name]['processors']['gaussfilter']['ampl_min_thr']
                dwfs_smooth = gaussian_filter(wfs, sigma, kernel_half_width, derivative=True)
                _maxima_tuples = [find_rel_maxima(dwf, wf, thr=ampl_min_thr) for dwf, wf in zip(dwfs_smooth, wfs_smooth)]
                df[wf_name+'_n_peaks'] = np.array( [ el[0] for el in _maxima_tuples ] )
                self.wfs_smooth[wf_name]['gaussfilter']['swfd'] = dwfs_smooth
            else:
                self.wfs_smooth[wf_name]['gaussfilter']['swfd'] = None
            #
        #
        return self.wfs_smooth
    #

    def procSingleEvent(self, wfs_bslnsubtr, raw_wfs):
        wfs_smooth_dict = dict()

        for wf_name in self.chs_map:
            try:
                sigma = self.chs_map[wf_name]['processors']['gaussfilter']['sigma']
                kernel_half_width = self.chs_map[wf_name]['processors']['gaussfilter']['kernel_half_width']
                _wf = wfs_bslnsubtr[wf_name].copy()
            except KeyError:
                continue
            #

            wfs_smooth_dict[wf_name] = {'gaussfilter':dict()}

            if ((_wf.ndim==2) and (_wf.shape[0]>1)) or (_wf.ndim>2):
                raise TypeError(f'The "raw_wf" must be a Numpy array corresponding to a single waveform, while it is an array of shape {_wf.shape}.')
            #

            _wf = _wf.flatten()

            wfs_smooth = gaussian_filter(_wf, sigma, kernel_half_width, derivative=False)
            wfs_smooth_dict[wf_name]['gaussfilter'] = {'swf':wfs_smooth}

            if ('derivative' in self.chs_map[wf_name]['processors']['gaussfilter']) and (self.chs_map[wf_name]['processors']['gaussfilter']['derivative']==True):
                wfs_smooth_deriv = gaussian_filter(_wf, sigma, kernel_half_width, derivative=True)
                wfs_smooth_dict[wf_name]['gaussfilter']['swfd'] = wfs_smooth_deriv
            else:
                wfs_smooth_dict[wf_name]['gaussfilter']['swfd'] = None
            #
        #
        return wfs_smooth_dict
    #
#