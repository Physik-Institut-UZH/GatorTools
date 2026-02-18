import sys

#The imports here below must be in the $PYTHONPATH
from GatorDaqProc import GatorDaqProc

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