import tools.trimmomaticClass as trim
import tools.vsearchClass as vsearch
import tools.diamondClass as Diamondpipe
import pipeline.d16spipelineClass as D16sPipe
import tools.rpobClass as RpobPipe
import tools.scgsClass as ScgsPipe
import quantification.normalizationClass as norm


import os

d16sPipe = D16sPipe.d16sPipe()
diamondpipe = Diamondpipe.diamondpipe()
rpobpipe = RpobPipe.rpobpipe()
scgspipe = ScgsPipe.scgspipe()

class PairedEnd():
    def __init__(self, data):
        self.info = ''
        self.pairedR1File = data['pairedR1File']
        self.pairedR2File = data['pairedR2File']
        self.bin = data['programs']
        self.sample_name = data['sample_output_file']
        self.data = data

    def run(self):
        
        #trim_name = self.pairedR1File.replace('.gz','')
        #if not os.path.exists(trim_name):
        #    print('Step 1: Trimming and QC using bbduk')
        #    if not trim.pairedEnd(self.bin, self.pairedR1File, self.pairedR2File):
        #        return 0
        #else:
        #    print('Skipping: bbduk output already exists.')
            
        #if not os.path.exists(trim_name):
        #    print('Step 1: Trimming and QC using Trimmomatic')
        #    if not trim.pairedEnd(self.bin, self.pairedR1File, self.pairedR2File):
        #        return 0
        #else:
        #    print('Skipping: Trimmomatic output already exists.')
        
        vsearch_name = self.sample_name + '.clean'
        if not os.path.exists(vsearch_name):
            print('\n\n\nStep 2: Merging paired end reads using Vsearch')
            if not vsearch.merge(self.bin, self.pairedR1File, self.pairedR2File, self.sample_name):
                return 0
        else:
            print('Skipping: Vseach output already exists!')

        print('\n\n\nStep 3: Run diamond for annotating reads')
        if not diamondpipe.run(self.sample_name+'.clean', self.data['diamond_parameters']):
            return 0

        d16s_name = self.sample_name + ".clean.sorted.bam.merged.quant"
        if not os.path.exists(d16s_name):
            print('\n\n\nStep 4: Normalize to 16S rRNAs - this may take a while ...')
            if not d16sPipe.run(self.sample_name+'.clean'):
                return 0
        else:
            print('Skipping: bowtie results already exists!')

        print('\n\n\nStep 5: Getting RpoB counts')
        if not rpobpipe.run(self.sample_name+'.clean'):
            return 0
        
        print('\n\n\nStep 6: Getting SCGs FPKM Values')
        if not scgspipe.run(self.sample_name+'.clean'):
            return 0

        print('\n\n\nStep 7: Normalization')
        if not norm.normalize(self.sample_name+'.clean',self.sample_name+'.clean.sorted.bam.merged.quant',self.sample_name+'.clean.{}.matches.quant'.format(self.data['diamond_parameters']['database']), 0.01, self.data['parameters']):
            return 0
