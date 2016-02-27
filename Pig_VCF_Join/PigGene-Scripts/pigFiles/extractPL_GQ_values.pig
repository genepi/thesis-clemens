/**
 * Pig script to filter the exome information 
 * and create columns for each of the PL-values 
 * and a column for the GQ-value. Stores the
 * output into the specified folder.
 * 
 * call this script like this:
 * pig -param input=GeneSamples/in/6exomes.vcf -param output=GeneSamples/output extractPL_GQ_values.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
 
 REGISTER pigGene.jar;
 
 in = LOAD '$input' USING pigGene.PigGeneStorage();
 out = FOREACH in {
  genSplit = STRSPLIT(genotype,':');
  pl_split = STRSPLIT(genSplit.$1,',');
  GENERATE *, FLATTEN(pl_split.$0) AS (plSplit1:int), FLATTEN(pl_split.$1) AS (plSplit2:int), 
  		FLATTEN(pl_split.$2) AS (plSplit3:int), FLATTEN(genSplit.$2) AS (quality:int);
 };

 STORE out INTO '$output';