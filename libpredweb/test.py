#!/usr/bin/env python
import os
import sys
from libpredweb import webserver_common as webcom
from libpredweb import myfunc
if __name__ == '__main__':
    progname=os.path.basename(sys.argv[0])
    general_usage = """
    usage: %s TESTMODE options
    """%(sys.argv[0])
    numArgv = len(sys.argv)
    if numArgv <= 1:
        print(general_usage)
        sys.exit(1)
    TESTMODE=sys.argv[1]


    if TESTMODE == "readcomputenode":
        infile = sys.argv[2]
        dt = webcom.ReadComputeNode(infile)
        print(dt)

    if TESTMODE == "pdb2seq":
        pdbfile = sys.argv[2]
        seq = myfunc.PDB2Seq(pdbfile)
        print(seq)

    if TESTMODE == "urlretrieve":
        url = sys.argv[2]
        outfile = sys.argv[3]
        timeout = int(sys.argv[4])
        try:
            myfunc.urlretrieve(url, outfile, timeout)
        except Exception as e:
            print("retrieve %s failed with errmsg=%s"%(url, str(e)) )

