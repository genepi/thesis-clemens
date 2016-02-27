/**
 * Pig script to filter just the first
 * 15 lines of the sample1 file and store
 * these 15 lines to a new file. 
 * 
 * call this script like this:
 * pig -param sample=GeneSamples/unmerged/sample1.vcf -param output=GeneSamples/out only15Lines.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */

REGISTER pigGene.jar;
sample1 = LOAD '$sample' USING pigGene.PigGeneStorageUnmerged();
lessLines = FILTER sample1 BY pigGene.FilterLines(chrom);

STORE lessLines INTO '$output';