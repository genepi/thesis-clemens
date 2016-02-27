/**
 * Pig script loads multiple files from a given
 * directory and uses my UDF to add the corresponding
 * filenames as a new column to each line. Finally 
 * this script filters the values that are lower a
 * given threshold and dumps the relation on screen.
 * 
 * call this script like this:
 * pig -param folder=GeneSamples/input -param thres=20 getFolderPath.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
 
REGISTER pigGene.jar;
data = LOAD '$folder' USING pigGene.PigStorageWithFilename() AS (id:int,name:chararray,filename:chararray);
out = FILTER data BY id <= $thres;
DUMP out;