/**
 * Pig script imports multiple samplefiles,
 * filters chromosomes and sorts the output
 * relation by CHROM POS REF ALT SAMPLE. 
 * 
 * call this script like this:
 * pig -param input=GeneSamples/unmerged -param outOrder=GeneSamples/outOrder -param output=GeneSamples/out multipleSamples.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
 
REGISTER pigGene.jar;
samples = LOAD '$input' USING pigGene.PigGeneStorageUnmerged(); 

/* filter header and chromosome & project unused columns */
samp = FILTER samples BY pigGene.IgnoreHeader(chrom);
sampf = FILTER samp BY pigGene.FilterChromosome(chrom);
sfu = FOREACH sampf GENERATE chrom, pos, ref, alt, file;

/* ordering */
ordered = ORDER sfu BY chrom, pos, ref, alt, file ASC;
STORE ordered INTO '$outOrder';

/* remove duplicates */
duplicates = FOREACH ordered GENERATE chrom, pos, ref, alt;
noDup = DISTINCT duplicates;
STORE noDup INTO '$output';