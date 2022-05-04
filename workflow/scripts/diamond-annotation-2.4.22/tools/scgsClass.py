#scgsClass

import os
import sys
import pandas as pd

class scgspipe():
    def __init__(self, scgspath="/home/data2/influent_litreview_analyses/nist_meta_workflow/workflow/scripts/diamond-annotation-2.4.22/bin/scgs/dataset"):
        self.info = ""
        self.scgsdb = scgspath
        print(sys.path)
        
    def run(self,fi):
        try:
            os.popen(" ".join(
                ['diamond',
                 'blastx',
                 '-e 0.00001',
                 '--id 80',
                 '--query-cover 80',
                 '-k 1',
                 '-d', self.scgsdb + '.dmnd',
                 '-q', fi,
                 '-o', fi + '.scgs.matches',
                 '--outfmt 6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore qlen slen'
                 ])).read()            

            file1 = pd.read_csv(fi+'.scgs.matches',sep='\t',header=None)
            column= ['qseqid', 'sseqid', 'pident' ,'length' ,'mismatch', 'gapopen' ,'qstart', 'qend', 'sstart', 'send' ,'evalue', 'bitscore','qlen','slen']
            file1.columns = column
            file1.sort_values(['qseqid','bitscore'],inplace=True,ascending=False) #sort values on bitscore
            file1.drop_duplicates(subset='qseqid',keep='first',inplace=True)
            file1.to_csv(fi+'.scgs.matches',sep='\t',index=False,header=None)
            file1 = file1[file1['pident']>= 80.0]
            file1= file1[file1['length']>= 17]
            file2 =file1.groupby(['sseqid','slen']).size().reset_index(name='count')
            file2['gene-len(bp)'] = (file2['slen']+1)*3
            #FPKM SCGS
            file2['FPKM_Normalization'] = (file2['count']/file2['count'].sum())/file2['gene-len(bp)']
            file2['FPKM_Normalization'] = file2['FPKM_Normalization'].multiply(1000000000)
            file2.to_csv(fi+'.scgs.matches.quant',sep='\t',index=False)
            return True
        except:
            return False
