package NaiveVariantCaller;

import htsjdk.samtools.SAMRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.seqdoop.hadoop_bam.FileVirtualSplit;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import utils.NaiveVariantCallerKeyWritable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */
public class NaiveVariantCaller_Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, SAMRecordWritable, NaiveVariantCallerKeyWritable, Text> {
    private static final int MIN_BASE_QUAL = 30;
    private static final int MIN_MAP_QUAL = 30;
    private static final int MIN_ALIGN_QUAL = 30;

    private static final int MIN_READ_LENGTH = 25;

    private static final char BASE_A = 'A';
    private static final char BASE_C = 'C';
    private static final char BASE_G = 'G';
    private static final char BASE_T = 'T';

    @Override
    protected void map(LongWritable key, SAMRecordWritable value, Context context) throws IOException, InterruptedException {
        SAMRecord samRecord = value.get();
        final String sampleIdentifier = samRecord.getHeader().getReadGroups().get(0).getSample();
        final byte[] readBases = samRecord.getReadBases();
        String sequence = new String(readBases, StandardCharsets.UTF_8);

        if (readFullfillsRequirements(samRecord)) {
            for (int i = 0; i < sequence.length(); i++) {
                if (baseQualitySufficient((samRecord.getBaseQualities()[i]))) {
                    NaiveVariantCallerKeyWritable outputKey = new NaiveVariantCallerKeyWritable(sampleIdentifier, samRecord.getReferencePositionAtReadPosition(i+1));
                    switch (sequence.charAt(i)) {
                        case BASE_A:
                            context.write(outputKey, new Text(String.valueOf(BASE_A)));
                            break;
                        case BASE_C:
                            context.write(outputKey, new Text(String.valueOf(BASE_C)));
                            break;
                        case BASE_G:
                            context.write(outputKey, new Text(String.valueOf(BASE_G)));
                            break;
                        case BASE_T:
                            context.write(outputKey, new Text(String.valueOf(BASE_T)));
                            break;
                        default:
                            System.out.println("other base character occurred at position " + outputKey);
                            break;
                    }
                }
            }
        }
    }

    private boolean readFullfillsRequirements(SAMRecord samRecord) {
        return mappingQualitySufficient(samRecord.getMappingQuality())
                && alignmentQualitySufficient(samRecord)
                && !samRecord.getReadUnmappedFlag()
                && !samRecord.getDuplicateReadFlag()
                && samRecord.getReadLength() > MIN_READ_LENGTH;
    }

    private boolean mappingQualitySufficient(int mapQual) {
        // 255 means that mapping quality is not available
        if(mapQual >= MIN_MAP_QUAL || mapQual == 255) {
            return true;
        }
        return false;
    }

    private boolean alignmentQualitySufficient(SAMRecord samRecord) {
        try {
            int alignmentQuality = samRecord.getIntegerAttribute("AS");
            if(alignmentQuality >= MIN_ALIGN_QUAL) {
                return true;
            }
        } catch (NullPointerException e) {
            //attribute value is not mandatory
            //therefore return true
            return true;
        }
        return false;
    }

    private boolean baseQualitySufficient(int baseQual) {
        if(baseQual >= MIN_BASE_QUAL) {
            return true;
        }
        return false;
    }

}