# WW_influent_bias
Utilizes Snakemake to retrieve samples from NCBI by SRA, taxonomically profile and genetically profile ARGs

The purpopse of this workflow is to be a server deployable modular workflow for the assesment of anitbiotic resistance and taxanomy via 3 different tools : kraken, centrifuge and metaphlan. 
Users will have to integrate their own database locations in the snakemake config file. 
