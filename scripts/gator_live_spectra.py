#!/usr/bin/env python

import sys

#The imports here below must be in the $PYTHONPATH
from GatorTools import GatorLiveSpectrum

def main():
    if len(sys.argv)>1:
        config_fname = sys.argv[1]
        spectrum_obj = GatorLiveSpectrum(config_fname)
    else:
        spectrum_obj = GatorLiveSpectrum()
    #

    spectrum_obj.run()

if __name__ == "__main__":
    main()