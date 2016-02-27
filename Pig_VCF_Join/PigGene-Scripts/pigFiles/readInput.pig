/**
 * Pig script to test my selfwritten
 * Loader Function. 
 * 
 * call this script like this:
 * pig -param input=GeneSamples/input/readInput.txt readInput.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
 
REGISTER pigGene.jar;
data = LOAD '$input' USING pigGene.storage.merged.PigGeneStorage();
DUMP data;