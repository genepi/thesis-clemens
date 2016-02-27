/**
 * Pig script calculates the median of 
 * the quality from the sample file.
 * 
 * call this script like this:
 * pig -param sample=GeneSamples/unmerged/sample1.vcf medianDataFu.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */

REGISTER pigGene.jar;
REGISTER datafu.jar;
define Median datafu.pig.stats.Median();

sample1 = LOAD '$sample' USING pigGene.PigGeneStorageUnmerged();
sample1Med = FOREACH sample1 GENERATE qual;
sample1MedGrouped = GROUP sample1Med ALL;

median = FOREACH sample1MedGrouped { 
	sorted = ORDER sample1Med BY qual; 
	GENERATE Median(sorted.qual);
};  
	
DUMP median;