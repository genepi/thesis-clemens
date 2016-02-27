/**
 * Pig script to gain the rsNumber from the reference file and
 * write it into the corresponding  id-column of the sample file.
 * Finally store the new generated relation (including the rsNumber). 
 * 
 * call this script like this:
 * pig -param sample=GeneSamples/unmerged/sample1.vcf -param ref=GeneRefFile/00-All.vcf -param output=GeneSamples/out getRsNumber.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */

REGISTER pigGene.jar;
sample1 = LOAD '$sample' USING pigGene.PigGeneStorageUnmerged();
refFile = LOAD '$ref' USING PigStorage('\t')
			AS (REFchrom:chararray, REFpos:int, REFid:chararray, REFr:chararray, 
						REFalt:chararray, REFqual:float, REFfilt:chararray, REFinfo:chararray);

rfp = FOREACH refFile GENERATE REFchrom, REFpos, REFid; /* reduce data amount just before filtering and joining */
s1f = FILTER sample1 BY pigGene.IgnoreHeader(chrom);
rff = FILTER rfp BY pigGene.IgnoreHeader(REFchrom);

/* if chrome and pos match: add rsNumber to the sample1 file */
joined = JOIN s1f BY (chrom,pos), rff BY (REFchrom,REFpos);
reordered = FOREACH joined GENERATE chrom,pos,REFid,ref,alt,qual,filt,info,format,genotype;

STORE reordered INTO '$output';