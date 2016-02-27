/**
 * Pig script to test my own
 * Storage implementation.
 * 
 * call this script like this:
 * pig -param input=GeneSamples/input/readInput.txt pigGeneLoaderTest.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */


REGISTER pigGene.jar;

in = LOAD '$input' USING pigGene.PigGeneStorage();
DUMP in;