import os
import sys
import json
import glob
import time
import stat
import paramiko

import logging
from logging.handlers import TimedRotatingFileHandler

from GatorUtils import setup_logger


class GatorDaqSync:
    FILES_EXT = {".root", ".json", ".txt"}
    SYNC_STATE_FNAME = '.sync_state'

    def __init__(self, config_fpath:str=""):
        if config_fpath=="":
            self.config_fpath = self._search_config_file()
            if self.config_fpath is None:
                raise FileNotFoundError('Could not find the configuration for the data sync config file.')
        else:
            self.config_fpath = config_fpath
        
        with open(self.config_fpath, "r") as f:
            config_dict = json.load(f)
        

        self.local_base_dir = config_dict['local_base_dir']
        self.remote_host = config_dict['remote_host']
        self.username = config_dict['username']
        self.ssh_key_file = config_dict['ssh_key_file']
        self.remote_base_dir = config_dict['remote_base_dir']

        self.loop_sleep_sec = 600 #Default sleep is 10 mins
        if('loop_sleep_sec' in config_dict):
            self.loop_sleep_sec = int(config_dict['loop_sleep_sec'])

        if 'logging' in config_dict:
            self.logger = setup_logger(config_dict['logging'])
        else:
            self.logger = setup_logger()

        self.ssh_client = None
        self.sftp_client = None

    
    def _search_config_file(self):
        #Check if it is encoded in an environment variable
        conf_file = os.environ.get("GATOR_DATASYNCCONF_FILE")
        
        if not conf_file is None:
            if os.path.exists(conf_file) and os.path.isfile():
                return conf_file
            else:
                return None
        
        #Check inside the $HOME/local/etc/GatorDaqSync directory
        home_path = os.environ.get("HOME")
        settings_dir = os.path.join(home_path, 'local', 'etc', 'GatorDaqSync')

        if os.path.exists(settings_dir) and os.path.isdir(settings_dir):
            fpath = os.path.join(settings_dir, 'config.json')
            if os.path.exists(fpath) and os.path.isfile(fpath):
                return fpath
            #
        #

        return None

    def _load_sync_state_file(self, _path):
        if not os.path.exists(_path):
            return {}
        try:
            with open(_path, "r") as f:
                return json.load(f)
        except Exception as err:
            self.logger.error(f'GatorDaqSync.load_transferred_file: failed to read the sync state file "{_path}": {err}', exc_info=True)
            return {}

    def _save_sync_state_file(self, sync_state_fpath, sync_state_dict):
        with open(sync_state_fpath, "w") as f:
            json.dump(sync_state_dict, f, indent=2)

    def connect(self):
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            self.ssh_client.connect(self.remote_host,
                                username=self.username,
                                key_filename=self.ssh_key_file
                                )
            self.logger.info('GatorDaqSync.connect: ssh client connected successfully')
        except Exception as err:
            self.logger.critical(f'GatorDaqSync.connect: failed to establish ssh connection with the remote server: {err}', exc_info=True)
            return None, None
        #

        try:
            self.sftp_client = self.ssh_client.open_sftp()
            self.logger.info('GatorDaqSync.connect: sftp client connected successfully')
        except Exception as err:
            self.logger.critical(f'GatorDaqSync.connect: failed to open the sftp connection with the remote server: {err}', exc_info=True)
            return self.ssh_client, None
            
        return self.ssh_client, self.sftp_client

    def _ensure_remote_dirs(self, remote_dir:str, check_only:bool):
        """
        Ensure that the remote directory exists.

        Parameters:
            remote_dir (str): full remote directory path
            check_only (bool): 
                - True  → only verify, never create; return False if path missing
                - False → create missing directories as needed

        Returns:
            bool: True if directory now exists (or existed), False if missing and check_only=True
        """
        if (remote_dir=="/") or (remote_dir==""):
            return True  # root always exists

        relpath = os.path.relpath(remote_dir, self.remote_base_dir)

        parts = relpath.strip("/").split("/")
        path = self.remote_base_dir #This must exist and shall not be created
        for p in parts:
            if not p:
                continue

            path = path + '/' + p
            
            try:
                st = self.sftp_client.stat(path)
                if not stat.S_ISDIR(st.st_mode):
                    # Exists but is not a directory → fatal
                    self.logger.error(f'GatorDaqSync._ensure_remote_dirs: the path "{path}" is not a directory as expected. Cannot continue chcking the existence of the destination other sub-directories "{remote_dir}"', file=sys.stderr)
                    return False
            
            except FileNotFoundError:
                # Directory does not exist
                if check_only:
                    return False
                # Create it
                try:
                    self.logger.info(f'GatorDaqSync._ensure_remote_dirs:making directory "{path}"')
                    self.sftp_client.mkdir(path)
                except IOError as err:
                    # Creation failed → remote FS changed or permission denied
                    self.logger.error(f'GatorDaqSync._ensure_remote_dirs: failed to create directory "{path}" while checking the existence of the destination directory "{remote_dir}": {err}', exc_info=True)
                    return False
        return True
            
    def _file_needs_upload(self, local_f_path, remote_f_path, sync_state_dict):
        """
        Determine whether a local file needs to be uploaded to the remote host.

        Conditions requiring upload:
          - Not present in sync_state_dict
          - Local size differs from sync_state_dict
          - Local mtime is newer than sync_state_dict
          - Remote file mtime is older than local file mtime
        """
        fname = os.path.basename(local_f_path)
        sync_state = sync_state_dict.get(fname)

        # Fetch local stat
        local_stat = os.stat(local_f_path)

        # if not in log → upload
        if sync_state is None:
            return True

        # If changed relative to saved sync state → upload
        if local_stat.st_size != sync_state["size"]:
            return True
        
        if int(local_stat.st_mtime) > int(sync_state["mtime"]):
            return True

        # Check if remote file exists using listdir (no exceptions)
        remote_dir = os.path.dirname(remote_f_path)
        try:
            remote_f_exists = fname in self.sftp_client.listdir(remote_dir)
        except IOError:
            # remote directory missing? then treat as "file does not exist"
            remote_f_exists = False
        
        # It means that the file was transferred and already archived
        if not remote_f_exists:
            return False

        # Remote file exists → compare mtimes
        try:
            remote_stat = self.sftp_client.stat(remote_f_path)
        except IOError:
            # Although we just checked listdir, race conditions can happen and the file being archived in the meanwhile
            return False
        
        # Remote file older than local → upload
        if int(remote_stat.st_mtime) < int(local_stat.st_mtime):
            return True

        return False

    def _sync_directory(self, relpath, f_list):
        dirpath = os.path.join(self.local_base_dir, relpath)
        self.logger.info(f'GatorDaqSync._sync_directory: synchronizing directory {dirpath} ({len(f_list)} files)')

        # Build remote directory path
        remote_dir = os.path.join(self.remote_base_dir, relpath)
        
        sync_state_fpath = os.path.join(dirpath, GatorDaqSync.SYNC_STATE_FNAME)
        sync_state_dict = self._load_sync_state_file(sync_state_fpath)

        for fname in f_list:
            skip_file = False
            local_f_path = os.path.join(dirpath, fname)
            remote_f_path = os.path.join(remote_dir, fname)

            if fname in sync_state_dict:
                # Just check if the remote directory exists. If it doesn't it means that it was archived already
                if not self._ensure_remote_dirs(remote_dir, check_only=True):
                    skip_file = True
            else:
                # The file was never transferred: if the remote dir does not exist make it
                if not self._ensure_remote_dirs(remote_dir, check_only=False):
                    # Here there is an error!
                    skip_file = True
            #

            if (not skip_file) and self._file_needs_upload(local_f_path, remote_f_path, sync_state_dict):
                
                sync_fail = False

                local_st = os.stat(local_f_path)

                self.logger.info(f"GatorDaqSync._sync_directory: [UPLOAD] {local_f_path}")

                tmp = remote_f_path + ".part"
                unixtime = time.time()
                self.sftp_client.put(local_f_path, tmp)
                self.sftp_client.chmod(tmp, stat.S_IMODE(local_st.st_mode))
                self.sftp_client.utime(tmp, (local_st.st_atime, local_st.st_mtime))

                try:
                    self.sftp_client.stat(tmp)
                except FileNotFoundError as err:
                    self.logger.error(f'GatorDaqSync._sync_directory: temporary file "{tmp}" vanished before rename: {err}', exc_info=True)
                    sync_fail = True
                #

                #Check before whether the destination file already exists
                if (not sync_fail) and (fname in self.sftp_client.listdir(remote_dir)):
                    try:
                        self.sftp_client.remove(remote_f_path)
                    except Exception as err:
                        self.logger.error(f'GatorDaqSync._sync_directory: failed to remove the outdated file "{remote_f_path}" from the destination directory. Cannot finish synchronization of the local file "{local_f_path}". A ".part" file may be left over in the destination directory and may need of manual removal', exc_info=True)
                        sync_fail = True

                if not sync_fail:
                    self.sftp_client.rename(tmp, remote_f_path)
                    
                    st = os.stat(local_f_path)
                    sync_state_dict[fname] = {
                        "unixtime": int(unixtime),
                        "mtime": int(st.st_mtime),
                        "size": st.st_size
                        }
                    # save updated per-file sync status. Many overwrites, but much safer
                    self._save_sync_state_file(sync_state_fpath, sync_state_dict)
            else:
                self.logger.debug(f"GatorDaqSync._sync_directory: [SKIP] {local_f_path}")

        
    
    def _sync_tree(self):
        # First change directory
        os.chdir(self.local_base_dir)

        for dirpath, dirnames, filenames in os.walk('.'):
            # relative path for remote
            relpath = os.path.relpath(dirpath, self.local_base_dir)
            relpath = os.path.relpath(dirpath, '.')
            depth = 0 if relpath == "." else relpath.count(os.sep) + 1
    
            if depth >=2:
                dirnames[:] = []

            f_list = [fname for fname in filenames if (os.path.splitext(fname)[1] in GatorDaqSync.FILES_EXT)]
            if len(f_list)==0:
                continue
            
            self._sync_directory(relpath, f_list)
    
    def sync_loop(self):
        try:
            while True:
                self.logger.info('GatorDaqSync.sync_loop: start loop')
                ssh, sftp = self.connect()
                if (not ssh is None) and (not sftp is None):
                    try:
                        self._sync_tree()
                    finally:
                        # Ensure connections are closed even if sync_tree fails
                        if self.sftp_client:
                            self.sftp_client.close()
                            self.sftp_client = None
                        if self.ssh_client:
                            self.ssh_client.close()
                            self.ssh_client = None
                        #
                time.sleep(self.loop_sleep_sec)
        except KeyboardInterrupt:
            self.logger.info("GatorDaqSync.sync_loop: interrupted by user (Ctrl+C), exiting gracefully")
            # Optionally close connections if still open
            if self.sftp_client:
                self.sftp_client.close()
            if self.ssh_client:
                self.ssh_client.close()
            return

def main():
    if len(sys.argv)>1:
        config_fname = sys.argv[1]
        sync_client = GatorDaqSync(config_fname)
    else:
        sync_client = GatorDaqSync()
    

    sync_client.sync_loop()

if __name__ == "__main__":
    main()
