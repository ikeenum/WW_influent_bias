from scripts.common import diamond_annotation_input, get_diamond_annotation_string
localrules:
    diamond,
    annotation_normalize_card,
    annotation_normalize_Virdb,
    annotation_normalize_mobileOG,
    consolidate_card_results,
    consolidate_Virdb_results,
    consolidate_mobileOG_results

##### classification master rule #####

rule diamond:
    input:
        diamond_annotation_input(config)

rule annotation_normalize_card:
    """ Runs diamond and bowtie in order to align reads to the specified database and normalize"""
    input:
        R1=expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R1{preprocess}.fastq.gz",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS),
        R2=expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R2{preprocess}.fastq.gz",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS),
    output:
        temp(expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R1{preprocess}.fastq.gz.merged",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS)), 
        temp(expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R1{preprocess}.fastq.gz.unmerged",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS)),
        temp(expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R2{preprocess}.fastq.gz.unmerged",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS)),
        expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.card.matches.quant.normalization",
                 results_path = config["paths"]["results"],preprocess=PREPROCESS),
       # temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean",
       #       results_path = config["paths"]["results"],preprocess=PREPROCESS)),
       # temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.bam",
       #        results_path = config["paths"]["results"],preprocess=PREPROCESS)),
       # temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.card.matches",
       #        results_path = config["paths"]["results"],preprocess=PREPROCESS)),
       # temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.rpob.matches",
       #        results_path = config["paths"]["results"],preprocess=PREPROCESS)),
       # temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.sam",
       #        results_path = config["paths"]["results"],preprocess=PREPROCESS)),
       # temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.scgs.matches",
       #        results_path = config["paths"]["results"],preprocess=PREPROCESS)),
       # temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.sorted.bam",
       #        results_path = config["paths"]["results"],preprocess=PREPROCESS)),
       # temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.sorted.bam.merged",
       #        results_path = config["paths"]["results"],preprocess=PREPROCESS)),
       # temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.sorted.bam.merged.quant",
       #       results_path = config["paths"]["results"],preprocess=PREPROCESS))
    log:
        expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.card.log",
                  results_path=config["paths"]["results"],preprocess=PREPROCESS)
    conda:
        "../envs/diamond_annotate.yml"
    threads: 20
    group: "ncbi_cent_diamond"
    params: 
        diamond_annotate_string=get_diamond_annotation_string(config),
        final=expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}",
                  results_path = config["paths"]["results"],preprocess=PREPROCESS)
    shell:
        """
           python /home/data2/influent_litreview_analyses/nist_meta_workflow/workflow/scripts/diamond-annotation-2.4.22/diamond_pipeline.py \
           --forward_pe_file  {input.R1}\
           --reverse_pe_file  {input.R2}  \
           --output_file {params.final} \
           --database card \
           {params.diamond_annotate_string} > {log} 2>&1 

           touch {output}
        """



rule annotation_normalize_Virdb:
    """ Runs diamond and bowtie in order to align reads to the specified database and normalize"""
    input:
        R1=expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R1{preprocess}.fastq.gz",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS),
        R2=expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R2{preprocess}.fastq.gz",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS),
        merged=expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R1{preprocess}.fastq.gz.merged",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS),
        R1_unmerged=expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R1{preprocess}.fastq.gz.unmerged",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS),
        R2_unmerged=expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R2{preprocess}.fastq.gz.unmerged",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS)
    output:
        expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.vfdb.matches.quant.normalization",
                  results_path = config["paths"]["results"],preprocess=PREPROCESS),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.bam",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.vfdb.matches",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.rpob.matches",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.sam",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.scgs.matches",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.sorted.bam",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.sorted.bam.merged",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.sorted.bam.merged.quant",
               results_path = config["paths"]["results"],preprocess=PREPROCESS))

    log:
        expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.vfdb.log",
                  results_path=config["paths"]["results"],preprocess=PREPROCESS)
    conda:
        "../envs/diamond_annotate.yml"
    params: diamond_annotate_string=get_diamond_annotation_string(config),
            final=expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}",
                  results_path = config["paths"]["results"],preprocess=PREPROCESS)
    shell:
        """
           python /home/ikeenum/influent_litreview_analyses/nist_meta_workflow/workflow/scripts/diamond-annotation-2.4.22/diamond_pipeline.py  \
           --forward_pe_file  {input.R1}\
           --reverse_pe_file  {input.R2}  \
           --output_file {params.final} \
           --database vfdb \
           {params.diamond_annotate_string} > {log} 2>&1
        """

rule annotation_normalize_mobileOG:
    """ Runs diamond and bowtie in order to align reads to the specified database and normalize"""
    input:
        R1=expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R1{preprocess}.fastq.gz",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS),
        R2=expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R2{preprocess}.fastq.gz",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS),
        merged=expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R1{preprocess}.fastq.gz.merged",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS),
        R1_umerged=expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R1{preprocess}.fastq.gz.unmerged",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS),
        R2_unmerged=expand("{results_path}/intermediate/preprocess/{{sample}}_{{unit}}_R2{preprocess}.fastq.gz.unmerged",
                  results_path = config["paths"]["results"], preprocess=PREPROCESS)
    output:
        expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.mge.matches.quant.normalization",
                  results_path = config["paths"]["results"],preprocess=PREPROCESS),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.bam",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.mge.matches",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.rpob.matches",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.sam",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.scgs.matches",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.sorted.bam",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.sorted.bam.merged",
               results_path = config["paths"]["results"],preprocess=PREPROCESS)),
        temp(expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.clean.sorted.bam.merged.quant",
               results_path = config["paths"]["results"],preprocess=PREPROCESS))

    log:
        expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}.mge.log",
                  results_path=config["paths"]["results"],preprocess=PREPROCESS)
    conda:
        "../envs/diamond_annotate.yml"
    params: diamond_annotate_string=get_diamond_annotation_string(config),
            final=expand("{results_path}/diamond/{{sample}}_{{unit}}{preprocess}",
                  results_path = config["paths"]["results"],preprocess=PREPROCESS)
    shell:
        """
           python /home/ikeenum/influent_litreview_analyses/nist_meta_workflow/workflow/scripts/diamond-annotation-2.4.22/diamond_pipeline.py  \
           --forward_pe_file  {input.R1}\
           --reverse_pe_file  {input.R2}  \
           --output_file {params.final} \
           --database mge \
           {params.diamond_annotate_string} > {log} 2>&1
        """

rule consolidate_card_results:
    input: 
        expand("{results_path}/diamond/{sample}_{unit}{preprocess}.clean.card.matches.quant.normalization",
                  results_path = config["paths"]["results"],preprocess=PREPROCESS, sample = samples, unit=1)
    output:
        synthesize = results+"/diamond/card_results.tsv",
    shell:
        """
        grep "," {input} > {output.synthesize}
        """



rule consolidate_Virdb_results:
    input:  
        expand("{results_path}/diamond/{sample}_{unit}{preprocess}.clean.vfdb.matches.quant.normalization",
                  results_path = config["paths"]["results"],preprocess=PREPROCESS, sample = samples, unit=1)
    params:
        path = config["paths"]["results"]
    output:
        synthesize = results+"/diamond/Virdb_results.tsv",
    shell:
        """
        grep "" {input} > {output.synthesize}
        """


rule consolidate_mobileOG_results:
    input:
        expand("{results_path}/diamond/{sample}_{unit}{preprocess}.clean.mge.matches.quant.normalization",
                  results_path = config["paths"]["results"],preprocess=PREPROCESS, sample = samples, unit=1)
    params:
        path = config["paths"]["results"]
    output:
        synthesize = results+"/diamond/mobileOG_results.tsv",
    shell:
        """
        grep "" {input} > {output.synthesize}
        """

