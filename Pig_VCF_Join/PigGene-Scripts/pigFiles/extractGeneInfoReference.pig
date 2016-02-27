/**
 * Pig script to extract the 
 * GENEINFO from the info column 
 * of the reference file.
 * 
 * call this script like this:
 * pig -param input=GeneRefFile/00-All.vcf -param output=GeneSamples/output extractGeneInfoReference.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */

 REGISTER pigGene.jar;
 
 in = LOAD '$input' USING PigStorage('\t') AS (REFchrom:chararray, REFpos:long, REFid:chararray, REFr:chararray, REFalt:chararray, REFqual:double, REFfilt:chararray, REFinfo:chararray);
 tmp = FILTER in BY pigGene.IgnoreHeader(REFchrom);
 out = FOREACH tmp GENERATE pigGene.ExtractGeneInfo(REFinfo) AS REFinfo;
 
 STORE out INTO '$output';