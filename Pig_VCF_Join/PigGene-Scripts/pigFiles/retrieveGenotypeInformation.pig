/**
 * Pig script to filter a specified range
 * of positions and to join the input file
 * with the refererce file to gain the
 * rs-number. This script outputs the 
 * chromosome, the position, the rs-number
 * and the genotype information
 * 
 * call this script like this:
 * pig -param input=GeneSamples/in/6exomes.vcf -param output=GeneSamples/output -param ref=GeneRefFile/00-All.vcf -param chr=12 -param start=51373184 -param end=51422349 -param accuracy=0 retrieveGenotypeInformation.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
 
REGISTER pigGene.jar;
data = LOAD '$input' USING pigGene.PigGeneStorage();
in = FOREACH data GENERATE chrom, pos, ref, alt, genotype, persID;
inFilter = FILTER in BY chrom == '$chr' AND pos >= $start-$accuracy AND pos <= $end+$accuracy;

referenceFile = LOAD '$ref' USING PigStorage('\t') AS (REFchrom:chararray, REFpos:long, REFid:chararray, REFr:chararray, REFalt:chararray, REFqual:double, REFfilt:chararray, REFinfo:chararray);
ref = FOREACH referenceFile GENERATE REFchrom, REFpos, REFid;
refFilter = FILTER ref BY REFchrom == '$chr' AND REFpos >= $start-$accuracy AND REFpos <= $end+$accuracy;

joined = JOIN inFilter BY (chrom,pos), refFilter BY (REFchrom,REFpos);
out = FOREACH joined GENERATE chrom, pos, REFid, pigGene.GenotypeMapping(ref,alt,SUBSTRING(genotype,0,3)), persID; 
STORE out INTO '$output';