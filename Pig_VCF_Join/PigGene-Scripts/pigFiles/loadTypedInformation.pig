/**
 * Pig script that uses a custom loader to retrieve
 * the inputdata in a typed format and with named columns.
 * 
 * call this script like this:
 * pig -param input=GeneSamples/input/typedInformation2.txt loadTypedInformation.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */

REGISTER pigGene.jar;
in = LOAD '$input' USING pigGene.PigGeneStorage();
ext = FOREACH in GENERATE exome, persID;
DUMP ext;