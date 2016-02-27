/**
 * Pig script to calculate the mean quality of
 * the information obtained by the sample file. 
 * 
 * call this script like this:
 * pig -param sample=GeneSamples/unmerged/sample1.vcf meanQuality.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */

REGISTER pigGene.jar;
sample1 = LOAD '$sample' USING pigGene.PigGeneStorageUnmerged();
 
qualValues = FOREACH sample1 GENERATE qual;
qualGrouped = GROUP qualValues ALL;
qualCalc = FOREACH qualGrouped GENERATE SUM(qualValues.qual), COUNT(qualValues);
mean = FOREACH qualGrouped GENERATE qualCalc.$0 / qualCalc.$1;

DUMP mean;