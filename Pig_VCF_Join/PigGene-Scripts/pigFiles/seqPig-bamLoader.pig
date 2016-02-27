/** usage:
  * pig -param input=/home/clemens/hadoop/input/HG00006.chrom20.bam seqPig-bamLoader.pig
  */

REGISTER SeqPig.jar;
REGISTER hadoop-bam-6.0.jar;
REGISTER sam-1.93.jar;
REGISTER picard-1.93.jar;
REGISTER variant-1.93.jar;
REGISTER tribble-1.93.jar;
REGISTER commons-jexl-2.1.1.jar;

R1 = LOAD '$input' USING fi.aalto.seqpig.io.BamLoader('yes');

BAMout = LIMIT R1 3;

DUMP BAMout;