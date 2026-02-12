import numpy as np
import pandas as pd

import uproot


class GatorRawFileHandler:
    def __init__(self,
                 fpath:str, #The path of the datafile
                 chs_lst: list, #The branch names of the waforms to read (wf0, wf1, etc)
                 dig_id:int = 0, #The digitizer number (usually 0 with only one digitizer -- default)
                 wfs_datatype = np.uint32 #The type of data type of the waveforms arrays as saved in their corresponding ROOT TBranch (usually unsigned 32 bit integers -- default).
                 ):
        self.n_chs = len(chs_lst)
        if self.n_chs==0:
            raise ValueError(f'Empty list of wfs names. At least one should be given.')
        #
        self.fpath = fpath
        self.dig_id = dig_id
        self.tree_name = 'dig_' + str(dig_id)
        self.wfs_datatype = wfs_datatype
        self.wfs = {wf_name:None for wf_name in chs_lst}

        self.df = None
        
        #
        self.wfs_on_memory = False
        self.data_loaded = False
    #
    def __str__(self):
        return str(self.fpath)

    def __call__(self, keep_wf=True):
        #This function loads the waveforms and also makes a dataframe with the basic raw data from the tree on the other columns
        if self.wfs_on_memory:
            return
        #

        with uproot.open(self.fpath) as rootfile:
            tree = rootfile[self.tree_name]
            for wf_name in self.wfs:
                waveforms = tree[wf_name].array(library="np").astype(self.wfs_datatype)
                self.wfs[wf_name] = waveforms.astype(np.float32)
            #

            runtimes = tree["RunTime"].array(library="np").astype(np.float32)
            evcounters = tree[f"EvCounter_{self.dig_id}"].array(library="np").astype(np.uint32)
            ttts = tree[f"TimeTrigTag_{self.dig_id}"].array(library="np").astype(np.uint32)
        #
        self.df = {'RunTime': runtimes,
                   'EvCounter': evcounters,
                   'TimeTrigTag': ttts
                  }
        
        self.df = pd.DataFrame(self.df)
        
        self.data_loaded = True

        if keep_wf:
            self.wfs_on_memory = True
            return
        
        self.releaseWfs()
    #

    def releaseWfs(self):
        if(not self.data_loaded):
            return
        #
        
        for wf_name in self.wfs:
            self.wfs[wf_name] = None
        #
        self.wfs_on_memory = False

            

    def loadWfs(self):
        if not self.data_loaded:
            #The first time the wfs must be calculated by a general call of the class object
            return self
        #

        if self.wfs_on_memory:
            #No need to reload them
            return self
        #

        with uproot.open(self.fpath) as rootfile:
            tree = rootfile[self.tree_name]
            for wf_name in self.wfs:
                waveforms = tree[wf_name].array(library="np").astype(self.wfs_datatype)
                self.wfs[wf_name] = waveforms.astype(np.float32)
            #
        #
        self.wfs_on_memory = True
        return self
    #

    def getDf(self):
        return self.df.copy()
    #
    
    def getWfs(self):
        #This has to always return a copy of the Wfs
        release_wfs = False
        if not self.wfs_on_memory:
            release_wfs = True
            self.loadWfs()
        #
        ret_wfs = {ch_name: wfs.copy() for ch_name, wfs in self.wfs.items()}

        if release_wfs:
            self.releaseWfs()

        return ret_wfs
    #

    def isLoaded(self):
        return self.data_loaded
    #

    def isWfsOnMem(self):
        return self.wfs_on_memory
    #
#