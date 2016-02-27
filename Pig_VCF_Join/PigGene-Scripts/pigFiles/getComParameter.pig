/**
 * Pig script gets two parameters as input. These parameters define the 
 * range which lines (their corresponding number) should be stored into 
 * the output file.
 * 
 * call this script like this:
 * pig -param input=GeneSamples/parameterInput.txt -param start=3 -param end=11 -param output=GeneSamples/out getComParameter.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
 
REGISTER pigGene.jar;

inputFile = LOAD '$input' AS (key:int, val:chararray);
outputFile = FILTER inputFile BY pigGene.FilterLineRange($start,$end);
STORE outputFile INTO '$output';