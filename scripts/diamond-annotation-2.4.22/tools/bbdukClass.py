import os
import sys


def pairedEnd(location, R1, R2):
    print(sys.path)
    try:
        os.popen(" ".join(
            [location+'bbduk/bbduk.sh -Xmx 85g ',
             'in1=', R1,
             'in2=', R2,
             'out1=', R1.replace('.gz',''),
             'out2=', R2.replace('.gz',''),
             't=20 qtrim=rl trimq=30 minlen=100 maxns=0 rieb=t |& tee -a',
             R1.replace('_1.fastq.gz')+'_bbduk_rpt.txt'
             ])).read()
        return True
    except:
        return False
