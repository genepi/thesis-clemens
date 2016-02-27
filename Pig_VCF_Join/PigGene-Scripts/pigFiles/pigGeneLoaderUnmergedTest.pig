/**
 * Pig script to test my own
 * Storage implementation of 
 * the Unmerged Files.
 * 
 * call this script like this:
 * pig -param input=GeneSamples/in/sample1.vcf pigGeneLoaderUnmergedTest.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */


REGISTER pigGene.jar;

in = LOAD '$input' USING pigGene.PigGeneStorageUnmerged(); 
filt = FOREACH in GENERATE pos,ref,file;
DUMP filt;