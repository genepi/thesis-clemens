/**
 * Pig script to test simple 
 * joins between two relations.
 * 
 * call this script like this:
 * pig -param input1=GeneSamples/simpleJoinInput1.txt -param input2=GeneSamples/simpleJoinInput2.txt -param input3=GeneSamples/simpleJoinInput3.txt -param input4=GeneSamples/simpleJoinInput4.txt simpleJoin.pig
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
 
leftRel = LOAD '$input1' USING PigStorage('\t') AS (l1:chararray,l2:int,l3:int);
rightRel = LOAD '$input2' USING PigStorage('\t') AS (r1:int,r2:chararray);

newRel = JOIN leftRel BY l3, rightRel BY r1;
DUMP newRel;

leftRel2 = LOAD '$input3' USING PigStorage('\t') AS (l1:int,l2:chararray,l3:int);
rightRel2 = LOAD '$input4' USING PigStorage('\t') AS (r1:int,r2:chararray,r3:int);

newRel2 = JOIN leftRel2 BY (l1,l2), rightRel2 BY (r1,r2);
DUMP newRel2;