/**
 * Pig script to test simple
 * joins between two relations.
 *
 * call this script like this:
 * pig -x local -param inputSmall=20.filtered.PASS.vcf -param inputBig=HRC.r1.GRCh37.autosomes.mac5.sites_PART_MINI.vcf -param output=joinedRes SimpleJoin.pig
 */


REGISTER PigGene-1.0.jar;

leftRel = LOAD '$inputBig' USING PigStorage('\t') AS (chrom:chararray, pos:long, id:chararray, ref:chararray, alt:chararray, qual:double, filt:chararray, info:chararray);

rightRel = LOAD '$inputBig' USING PigStorage('\t') AS (REFchrom:chararray, REFpos:long, REFid:chararray, REFref:chararray, REFalt:chararray, REFqual:double, REFfilt:chararray, REFinfo:chararray);

-- TODO implement the join btw. the 2 relations


header = FILTER leftRel BY pigGene.UDFs.ExtractHeader(chrom);
DUMP header;

STORE header INTO '$output';
