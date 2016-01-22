package NaiveVariantCaller;

import htsjdk.samtools.SAMRecord;
import org.apache.hadoop.io.LongWritable;
import org.seqdoop.hadoop_bam.FileVirtualSplit;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import utils.NaiveVariantCallerKeyWritable;
import utils.NaiveVariantCallerPositionWritable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 02.10.15.
 */
public class NaiveVariantCaller_Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, SAMRecordWritable, NaiveVariantCallerKeyWritable, NaiveVariantCallerPositionWritable> {
    private static final int MIN_BASE_QUAL = 30;
    private static final int MIN_MAP_QUAL = 30;
    private static final int MIN_ALIGN_QUAL = 30;

    private static final int MIN_READ_LENGTH = 25;

    private static final char BASE_A = 'A';
    private static final char BASE_C = 'C';
    private static final char BASE_G = 'G';
    private static final char BASE_T = 'T';

    private Map<NaiveVariantCallerKeyWritable, NaiveVariantCallerPositionWritable> keyMapping = new HashMap<NaiveVariantCallerKeyWritable, NaiveVariantCallerPositionWritable>();

    @Override
    protected void map(LongWritable key, SAMRecordWritable value, Context context) throws IOException, InterruptedException {
        final String sampleIdentifier = ((FileVirtualSplit)context.getInputSplit()).getPath().getName();

        SAMRecord samRecord = value.get();
        final byte[] readBases = samRecord.getReadBases();
        String sequence = new String(readBases, StandardCharsets.UTF_8);

        if (readFullfillsRequirements(samRecord)) {
            for (int i = 0; i < sequence.length(); i++) {
                if (baseQualitySufficient((samRecord.getBaseQualities()[i]))) {
                NaiveVariantCallerKeyWritable outputKey = new NaiveVariantCallerKeyWritable(sampleIdentifier, samRecord.getReferencePositionAtReadPosition(i+1));

                    if(!this.keyMapping.containsKey(outputKey)) {
                        this.keyMapping.put(outputKey, new NaiveVariantCallerPositionWritable());
                    }
                    final NaiveVariantCallerPositionWritable posCounter = this.keyMapping.get(outputKey);

                    switch (sequence.charAt(i)) {
                        case BASE_A:
                            posCounter.incrementBase_A();
                            break;
                        case BASE_C:
                            posCounter.incrementBase_C();
                            break;
                        case BASE_G:
                            posCounter.incrementBase_G();
                            break;
                        case BASE_T:
                            posCounter.incrementBase_T();
                            break;
                        default:
                            System.out.println("other base character occurred at position " + outputKey);
                            break;
                    }
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator it = this.keyMapping.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            context.write((NaiveVariantCallerKeyWritable)pair.getKey(), (NaiveVariantCallerPositionWritable)pair.getValue());
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