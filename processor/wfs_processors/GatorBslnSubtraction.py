import numpy as np

from .GatorWfsProc import GatorWfsProc

class GatorBslnSubtraction(GatorWfsProc):
    def _post_init(self):
        self.wfs_bslnsubtr = dict() #This is filled by the to doProc method only
    #
    
    def doProc(self, wfs_bslnsubtr, df, raw_wfs):
        self.wfs_bslnsubtr = dict()

        #Quantities for the calculation of the wfs maxima
        for wf_name in self.chs_map:
            if not ('bslnsubtr' in self.chs_map[wf_name]):
                continue
            #

            bslnsamps = self.chs_map[wf_name]['bslnsubtr']['bslnsamps']

            if raw_wfs[wf_name].ndim==1:
                raw_wfs[wf_name] = raw_wfs[wf_name][None, :]
            #
            wf_n_samps = raw_wfs[wf_name].shape[1]

            means = np.mean(raw_wfs[wf_name][:, :bslnsamps], axis=1)
            df[wf_name+'_bslns_mean'] = means
            df[wf_name+'_bslns_rms'] = np.std(raw_wfs[wf_name][:, :bslnsamps], axis=1)
            medians = np.median(raw_wfs[wf_name][:, :bslnsamps], axis=1)
            df[wf_name+'_bslns_med'] = medians
            df[wf_name+'_bslns_mad'] = np.median(np.abs(raw_wfs[wf_name][:, :bslnsamps] - medians[:, np.newaxis]), axis=1)

            # Make the wfs with corrected bslns
            bslns_meth = self.chs_map[wf_name]['bslnsubtr']['bsln_meth']
            if bslns_meth=='mean':
                bslns = means
            elif bslns_meth=='median':
                bslns = medians
            else:
                raise ValueError('Unexpected method for the baselines calculation ({bslns_meth}). The only implemented methods are "mean" and "median".')
            #
            
            wfs_corr = raw_wfs[wf_name] - bslns[:, None]
            
            if ('neg_pulse' in self.chs_map[wf_name]['bslnsubtr']) and (self.chs_map[wf_name]['bslnsubtr']['neg_pulse']==True):
                wfs_corr = -1.0*wfs_corr
            #
            self.wfs_bslnsubtr[wf_name] = {'bslnsubtr':wfs_corr}
            
            # Calculate the maxima of each wf
            samp_max_arr = np.argmax(wfs_corr, axis=1)
            df[wf_name+'_samp_max'] = samp_max_arr

            wfs_max_lst = []

            for iWf, samp_max in enumerate(samp_max_arr):
                if ((samp_max - 2) >= 0) and ((samp_max + 2) < wf_n_samps):
                    wfs_max_lst.append(np.mean(wfs_corr[iWf, samp_max-2:samp_max+3]))
                else:
                    wfs_max_lst.append(wfs_corr[iWf, samp_max])
                #
            #
            df[wf_name+'_ampl_max'] = wfs_max_lst
        #
        return self.wfs_bslnsubtr
    #

    def procSingleEvent(self, wfs_bslnsubtr:dict, raw_wfs:dict):
        wfs_bslnsubtr = dict()

        for wf_name in self.chs_map:
            if not ('bslnsubtr' in self.chs_map[wf_name]):
                continue
            #
            
            bslnsamps = self.chs_map[wf_name]['bslnsubtr']['bslnsamps']
            bslns_meth = self.chs_map[wf_name]['bslnsubtr']['bsln_meth']

            raw_wf = raw_wfs[wf_name]
            if ((raw_wf.ndim==2) and (raw_wf.shape[0]>1)) or (raw_wf.ndim>2):
                raise TypeError(f'The "raw_wf" must be a Numpy array corresponding to a single waveform, while it is an array of shape {raw_wf.shape}.')
            #

            #Do not modify the original array
            raw_wf = raw_wf.copy().flatten()
            if bslns_meth=='mean':
                bslns = np.mean(raw_wf[:bslnsamps])
            elif bslns_meth=='median':
                bslns = np.median(raw_wf[:bslnsamps])
            else:
                raise ValueError(f'Unexpected method for the baselines calculation ({bslns_meth}). The only inplemented methods are "mean" and "median".')
            #
            
            wf_corr = raw_wf - bslns
            if ('neg_pulse' in self.chs_map[wf_name]['bslnsubtr']) and (self.chs_map[wf_name]['bslnsubtr']['neg_pulse']==True):
                wf_corr = -1.0*wf_corr
            #
            wfs_bslnsubtr[wf_name] = {'bslnsubtr':wf_corr}
        #
        return wfs_bslnsubtr
    #
#