#!/usr/bin/env python

import sys


#The imports here below must be in the $PYTHONPATH
from GatorTools.SyncDaqFiles import GatorDaqSync

def main():
    if len(sys.argv)>1:
        config_fname = sys.argv[1]
        sync_client = GatorDaqSync(config_fname)
    else:
        sync_client = GatorDaqSync()
    #

    sync_client.sync_loop()

if __name__ == "__main__":
    main()