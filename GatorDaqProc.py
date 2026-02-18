import os
import sys
import json
import glob
import time
from pathlib import Path
import shutil

import numpy as np
import pandas as pd
import uproot

import logging
from logging.handlers import TimedRotatingFileHandler

from GatorUtils import setup_logger
from processor import GatorFileProcessor

class GatorDaqProc:
    FILES_EXT = {".root"}
    PROC_STATE_FNAME = '.proc_state' #This is only the name prefix
    
    def __init__(self, config_fpath:str=""):
        if config_fpath=="":
            self.config_fpath = self._search_config_file()
            if self.config_fpath is None:
                raise FileNotFoundError('Could not find the configuration for the data sync config file.')
        else:
            self.config_fpath = config_fpath
        
        with open(self.config_fpath, "r") as f:
            self.config_dict = json.load(f)
        #

        self.staging_base_dir = self.config_dict['StagingBaseDir']
        self.proc_base_dir = self.config_dict['ProcBaseDir']

        if 'ArchiveFiles' in self.config_dict:
            self.archive_base_dir = self.config_dict['ArchiveFiles']['BaseDir']
            if not 'TrigRateRequired' in self.config_dict['ArchiveFiles']:
                self.config_dict['ArchiveFiles']['TrigRateRequired'] = False
            #
        else:
            self.archive_base_dir = None
        #


        self.chsmap = self.config_dict['chs_map']

        self.loop_sleep_sec = 600 #Default sleep is 10 mins
        if('loop_sleep_sec' in self.config_dict):
            self.loop_sleep_sec = int(self.config_dict['loop_sleep_sec'])

        if 'logging' in self.config_dict:
            self.logger = setup_logger(self.config_dict['logging'])
        else:
            self.logger = setup_logger()
    #

    def _load_proc_state_file(self, _path):
        if not os.path.exists(_path):
            return {}
        try:
            with open(_path, "r") as f:
                return json.load(f)
        
        except Exception as err:
            self.logger.error(f'GatorDaqProc._load_sync_state_file: failed to read the proc state file "{_path}": {err}', exc_info=True)
            return {}
    #

    def _load_DAQ_config_file(self, _fpath):
        try:
            with open(_fpath,"r") as configfile:
                return json.load(configfile)
        except Exception as err:
            self.logger.error(f'GatorDaqProc._load_DAQ_config_file: failed to load the DAQ configuration file "{_fpath}": {err}', exc_info=True)
            return None
        #
    #

    def _save_proc_state_file(self, proc_state_fpath, proc_state_dict):
        with open(proc_state_fpath, "w") as f:
            json.dump(proc_state_dict, f, indent=2)
        #
    #

    def _search_config_file(self):
        #Check if it is encoded in an environment variable
        conf_file = os.environ.get("GATOR_DAQPROC_FILE")
        
        if not conf_file is None:
            if os.path.exists(conf_file) and os.path.isfile():
                return conf_file
            else:
                return None
        
        #Check inside the $HOME/.local/etc/GatorDaqProc directory
        home_path = os.environ.get("HOME")
        settings_dir = os.path.join(home_path, '.local', 'etc', 'GatorDaqProc')

        if os.path.exists(settings_dir) and os.path.isdir(settings_dir):
            fpath = os.path.join(settings_dir, 'config.json')
            if os.path.exists(fpath) and os.path.isfile(fpath):
                return fpath
            #
        #

        return None
    #

    def _ensure_dirs(self, dir_path, check_only: bool):
        dir_path = Path(dir_path)

        # Exists?
        if not dir_path.exists():
            if check_only:
                self.logger.error(f"GatorDaqProc._ensure_dirs: Directory does not exist: {dir_path}")
                return False
            try:
                dir_path.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"GatorDaqProc._ensure_dirs: Created directory: {dir_path}")
            except Exception as e:
                self.logger.error(f"GatorDaqProc._ensure_dirs: Failed to create directory {dir_path}: {e}")
                return False

        # Is a directory?
        if not dir_path.is_dir():
            self.logger.error(f"GatorDaqProc._ensure_dirs: Path exists but is not a directory: {dir_path}")
            return False

        # Writable?
        if not os.access(dir_path, os.W_OK):
            self.logger.error(f"GatorDaqProc._ensure_dirs: Directory is not writable: {dir_path}")
            return False

        return True
    #
        
    def run(self):
        try:
            while True:
                try:
                    self.logger.info(f'GatorDaqProc.run: start of processing of the "{self.staging_base_dir}" directory tree into the "{self.proc_base_dir}" directory tree of processed files.')
                    self.ProcTree()
                finally:
                    time.sleep(self.loop_sleep_sec)
                #
            #
        except KeyboardInterrupt:
            self.logger.info("GatorDaqProc.run: interrupted by user (Ctrl+C), exiting gracefully")
            return
    #

    def ProcTree(self):
        # First change directory
        os.chdir(self.staging_base_dir)

        for dirpath, dirnames, filenames in os.walk('.'):
            relpath = os.path.relpath(dirpath, '.')
            depth = 0 if relpath == "." else relpath.count(os.sep) + 1

            # Do not descend more than 2 levels, as the FMCDAQ software organizes the original file in dataset/run (at most)
            # If there are deeper directories it means they where produced manually and are not meant to be processed here in the SC
            if depth >=2:
                dirnames[:] = []

            f_list = [fname for fname in filenames if (os.path.splitext(fname)[1] in GatorDaqProc.FILES_EXT)]
            if len(f_list)==0:
                continue
            
            self.ProcDirectory(relpath, f_list)
        #
    #

    def ProcDirectory(self, relpath, f_list):
        dirpath = Path(self.staging_base_dir) / relpath
        self.logger.info(f'GatorDaqProc.ProcDirectory: synchronizing directory {dirpath} ({len(f_list)} files)')

        proc_dir = Path(self.proc_base_dir) / relpath

        # Build archived directory path'
        archive_files = False
        archive_dir = None
        if self.archive_base_dir is not None:
            archive_files = True
            archive_dir = Path(self.archive_base_dir) / relpath

            if not self._ensure_dirs(archive_dir, check_only=False):
                self.logger.warning(f'GatorDaqProc.ProcDirectory: failed to check and/or create the archive directory "{archive_dir}" for this run. The root files of this directory will not be archived')
                archive_files = False
            #
        #
        
        proc_state_fpath = dirpath / f"{GatorDaqProc.PROC_STATE_FNAME}_{dirpath.name}.json"
        proc_state_dict = self._load_proc_state_file(proc_state_fpath)

        #Load the json file used to run the DAQ
        daq_conf_flist = glob.glob(str(dirpath/'*.json'))

        if len(daq_conf_flist)!=1:
            self.logger.error(f'GatorDaqProc.ProcDirectory: found {len(daq_conf_flist)} json files in the "{dirpath}". Exactly one non-hidden DAQ configuration json file is required (the daq configuration file). Cannot proceed with the processing of this directory.')
            return
        #
        daq_conf_dict = self._load_DAQ_config_file(daq_conf_flist[0])
        
        if not self._ensure_dirs(proc_dir, check_only=False):
            self.logger.warning(f'GatorDaqProc.ProcDirectory: failed to ensure the existence of the processing directory "{proc_dir}". The files of the run "{dirpath.name}" will not be processed.')
            return
        #

        if (self.archive_base_dir is not None) and archive_files and (not (Path(archive_dir)/Path(daq_conf_flist[0]).name).exists()):
            self.ArchiveFile(Path(daq_conf_flist[0]), Path(archive_dir)/Path(daq_conf_flist[0]).name, move=False)
        #

        #Extend the DAQ json config with the processor config and save
        proc_conf_fpath = Path(proc_dir) / Path(daq_conf_flist[0]).name
        if not proc_conf_fpath.exists():
            try:
                with open(proc_conf_fpath,'w') as f:
                    json.dump({**daq_conf_dict, 'proc_config':self.config_dict},
                              f,
                              indent=2
                              )
                #
            except Exception:
                self.logger.exception(f'GatorDaqProc.ProcDirectory: filed to write the {str(Path(proc_dir)/Path(daq_conf_flist[0]).name)} configuration file in the directory of the processed files.')
            #
        #
        
        for fname in f_list:
            archive_this_file = archive_files
            process_this_file = True
            local_f_path = dirpath / fname

            if (fname in proc_state_dict) and (not Path(local_f_path).exists()):
                if (not 'TrigRate' in self.config_dict):
                    #The root file was archived and there is not a request of the trigger rate calculation
                    continue
                else:
                    if ('TrigRate' in proc_state_dict[fname]):
                        #The root file was archived and there is the trigger rate calculation was successful
                        continue
                    #
                #
            #

            if (fname in proc_state_dict):
                # The file was already processed and maybe the trigger rate needs to be processed
                process_this_file = False
                self.logger.debug(f'GatorDaqProc.ProcDirectory: the file "{fname}" was already processed.')
            #

            proc_res = self.ProcFile(fpath=local_f_path,
                                     proc_dir=proc_dir,
                                     daq_conf_dict=daq_conf_dict,
                                     trig_rate_only=(not process_this_file)
                                    )

            if process_this_file and (proc_res is None):
                self.logger.warning(f'GatorDaqProc.ProcDirectory: failed to process the "{local_f_path}" file.')
                continue
            #

            if process_this_file:
                proc_state_dict[fname] = dict(proc_timestamp = proc_res['timestamp'])
            #

            trigrate_dict = None # This means that the trigger rate was not processed for this file (or it has failed)
            if "TrigRate" in proc_res:
                trigrate_dict = dict()
                try:
                    trigrate_dict['proc_timestamp'] = proc_res['TrigRate']['proc_timestamp']
                    trigrate_dict['trig_timestamp'] = proc_res['TrigRate']['trig_timestamp']
                    trigrate_dict['trig_rate'] = proc_res['TrigRate']['trig_rate']
                    trigrate_dict['rate_err'] = proc_res['TrigRate']['trig_rate_err']
                except Exception:
                    trigrate_dict = None
                #
            #

            if trigrate_dict is not None:
                proc_state_dict[fname]['TrigRate'] = trigrate_dict
            #
            
            if self.config_dict['ArchiveFiles']['TrigRateRequired'] and (trigrate_dict is None):
                archive_this_file = False
            #

            if archive_this_file:
                archive_f_path = archive_dir / fname
                try:
                    self.ArchiveFile(local_f_path, archive_f_path, move=True)
                except Exception:
                    self.logger.exception(f'GatorDaqProc.ProcDirectory: failed to archive file "{Path(local_f_path)}"')
            #
        #

        # Update the status (json) file about the processing state of the directory with the proc_state_dict object
        self._save_proc_state_file(proc_state_fpath, proc_state_dict)
    #

    def ProcFile(self, fpath, proc_dir:Path, daq_conf_dict, trig_rate_only:bool=False):
        '''
        Note: this function assumes that the entire DAQ system consists of a single board (DT5724).
        Therefore the DAQ quantities (sampling rate and waveforms llength) are always taken from the board [0].
        If the system becomes more complex (more channels and boards) the entire logic must be changed (also 
        for the "GatorFileProcessor" class and the waveform processors classes as well).
        '''
        proc_dict = dict()

        try:
            if not trig_rate_only:
                fileProcessor = GatorFileProcessor(fpath=fpath, chs_map=self.chsmap)
        except Exception as err:
            self.logger.exception(f'GatorDaqProc.ProcFile: failed to instance the file processor for file "{fpath}".')
            return None
        #

        try:
            if not trig_rate_only:
                fileProcessor()
        except Exception as err:
            self.logger.exception(f'GatorDaqProc.ProcFile: failed to process the waveforms for file "{fpath}".')
            return None
        #

        #Get the unix timestamp
        if not trig_rate_only:
            proc_dict['timestamp'] = int(time.time()+0.5) #This tells when the processing has actually finished

        # Here I should have a dataframe to be used and dumped in the processed file
        proc_df = None

        if not trig_rate_only:
            proc_df = fileProcessor.getDf()
            proc_dict['df'] = proc_df
        #

        proc_dict['DaqSettings'] = daq_conf_dict

        fname = Path(fpath).with_suffix(".npy").name
        proc_fpath = Path(proc_dir) / fname

        data_export = None

        if not trig_rate_only:
            data_export = {
                "DaqSettings": daq_conf_dict,
                "ProcSettings": self.config_dict,
                "WfsLength": int(daq_conf_dict['boards'][0]["WfsLen"]),
                "Data":{
                    "Cols": list(proc_df.columns),
                    "Types": {col: str(proc_df[col].dtype) for col in proc_df.columns},
                    "Arr": proc_df.to_numpy(copy=True),
                }    
            }
        #

        #Add the metadata found in the rootfile
        try:
            metadata_dict = self.ReadMetadataFromRootFile(fpath)
        except Exception as err:
            self.logger.exception(f'GatorDaqProc.ProcFile: failed to read the DAQ metadata from file "{fpath}".')
            metadata_dict = None
        #

        if (not trig_rate_only) and (metadata_dict is not None):
            data_export.update(metadata_dict)
            proc_dict['daq_metadata'] = metadata_dict
        #

        if (not trig_rate_only):
            np.save(proc_fpath, data_export) #This implicitly uses pickles
            self.logger.info(f'GatorDaqProc.ProcFile: File "{Path(fpath).name}" successfully processed into "{proc_fpath}" numpy file.')
        #

        if ('TrigRate' in self.config_dict) and (metadata_dict is None):
            self.logger.error(f'GatorDaqProc.ProcFile: missing DAQ metadata. Cannot process the trigger rate from the "{fpath}" without these information.')
            return proc_dict
        #

        if trig_rate_only:
            self.logger.debug(f'GatorDaqProc.ProcFile: only trigger rate processing requested for file "{Path(fpath).name}". Loading processed data from "{proc_fpath}".')
            try:
                proc_dict['df'] = self.LoadDfFromProcessedFile(proc_fpath=proc_fpath) #This is the only thing that is needed from this dictionary
            except Exception:
                self.logger.exception(f'GatorDaqProc.ProcFile: failed to load the dataframe from the the processed file "{proc_fpath}".')
                return proc_dict
            #
        #

        if metadata_dict is not None:
            self.ProcTrigRate(proc_dict = proc_dict,
                          fname = Path(proc_fpath).name,
                          daq_metadata = {
                                            **metadata_dict,
                                            'WfsLength': int(daq_conf_dict['boards'][0]["WfsLen"])
                                          }
                          )
        

        return proc_dict
    #

    def ProcTrigRate(self,
                     proc_dict, #Dictionary that is going to summarize the run processor inside the hidden proc status json file
                     fname, #Needed only to compose the print out messages
                     daq_metadata #Temporary dictionary coming from the DAQ metadata and settings coming from the root file
                     ):
        
        try:
            trigrate_conf = self.config_dict['TrigRate']
        except KeyError:
            return
        #

        #Get the number of events
        _df = proc_dict['df'].copy()
        _df = _df[ (_df['wf1_energy_trap']>=float(trigrate_conf['MinTrapEnergy'])) & (_df['wf1_energy_trap']<=float(trigrate_conf['MaxTrapEnergy'])) ]

        n_evs_before_cuts = _df.shape[0]

        for query in trigrate_conf['Queries']:
            try:
                _df = _df.query(query)
            except Exception:
                self.logger.exception(f'GatorDaqProc.ProcTrigRate: Invalid query "{query}" in TrigRate configuration. Cannot proceed to the trigger rate calculation for "{fname}" file')
                return
            #
        #

        n_evs = _df.shape[0]
        n_evs_err = np.sqrt(n_evs)

        #Now calculate the effective run time
        runtime_eff = daq_metadata['FileRunTime'] - (n_evs_before_cuts-n_evs)/daq_metadata['SampFreq']*daq_metadata['WfsLength']

        trigrate_dict = dict(proc_timestamp=int(time.time()+0.5)) #This is useful only if the trigger rate is actually processed at a different time wrt the main processing time
        
        tstart = daq_metadata['StartUnixTime']
        tstop = daq_metadata['StopUnixTime']
        trigrate_dict['trig_timestamp'] = int((tstart+tstop)/2. + 0.5)
        trigrate_dict['trig_rate'] = n_evs/runtime_eff
        trigrate_dict['trig_rate_err'] = n_evs_err/runtime_eff

        proc_dict['TrigRate'] = trigrate_dict

        if 'TrigRateFile' in trigrate_conf:
            #Append one line to the trigrate archive file
            self.WriteTrigRate(trigrate_conf['TrigRateFile'], proc_dict)  
        #
    #

    def ReadMetadataFromRootFile(self, fpath):
        metadata_dict = {}
        with uproot.open(fpath) as f:
            meta = f["metadata"]

            metadata_dict['StartUnixTime'] = int(meta["StartUnixTime"])
            metadata_dict['StopUnixTime']  = int(meta["StopUnixTime"])
            metadata_dict['FileRunTime']   = float(meta["FileRunTime"])
            metadata_dict['SampFreq']      = float(meta["SampFreq_0"])
        #
        return metadata_dict
    #

    def LoadDfFromProcessedFile(self, proc_fpath):
        data = np.load(proc_fpath, allow_pickle=True).item()

        df_payload = data["Data"]

        cols  = df_payload["Cols"]
        types = df_payload["Types"]
        arr   = df_payload["Arr"]

        df = pd.DataFrame(arr, columns=cols)

        for col, dtype_str in types.items():
            df[col] = df[col].astype(dtype_str)
        #
        return df
    #

    def WriteTrigRate(self, fpath, proc_dict):
        try:
            trig_timestamp = proc_dict['TrigRate']['trig_timestamp']
            trig_rate = proc_dict['TrigRate']['trig_rate']
            trig_rate_err = proc_dict['TrigRate']['trig_rate_err']

            with open(fpath, 'a') as f:
                f.write(f'{int(trig_timestamp)}  {trig_rate}  {trig_rate_err}\n')
            #
        except Exception as err:
            self.logger.exception(f'GatorDaqProc.WriteTrigRate: failed to write the triger rate value on file "{fpath}".')
            return
        #
    #

    def ArchiveFile(self, local_f_path, archive_f_path, move:bool=False):
        src = Path(local_f_path)
        dst = Path(archive_f_path)

        if not src.exists():
            raise FileNotFoundError(f"Source file does not exist: {src}")
        
        # Ensure destination directory exists
        dst.parent.mkdir(parents=True, exist_ok=True)

        # Copy file (metadata preserved)
        shutil.copy2(src, dst)

        # Verify copy: size check
        src_size = src.stat().st_size
        dst_size = dst.stat().st_size
        if src_size != dst_size:
            raise IOError(f'Archive copy size mismatch for {src.name}')
        
        # Copy verified → remove original
        if move:
            src.unlink()
        self.logger.info(f'GatorDaqProc.ArchiveFile: file "{src.name}" successfully archived into "{dst}".')
        #
    #

def main():
    if len(sys.argv)>1:
        config_fname = sys.argv[1]
        daq_rate_obj = GatorDaqProc(config_fname)
    else:
        daq_rate_obj = GatorDaqProc()
    #

    daq_rate_obj.run()

if __name__ == "__main__":
    main()