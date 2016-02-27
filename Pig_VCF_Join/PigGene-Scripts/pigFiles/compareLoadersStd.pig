/**
 * Pig script to load a file and filter 
 * a specified range of positions.
 * 
 * call this script like this:
 * pig -param input=GeneSamples/in/6exomes.vcf -param output=GeneSamples/output -param chr=12 -param start=51373184 -param end=51422349 -param accuracy=0 compareLoadersStd.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
 
 REGISTER pigGene.jar;
 
 in = LOAD '$input' USING pigGene.PigGeneStorage();
 out = FILTER in BY chrom == '$chr' AND pos >= $start-$accuracy AND pos <= $end+$accuracy;
 
 STORE out INTO '$output';