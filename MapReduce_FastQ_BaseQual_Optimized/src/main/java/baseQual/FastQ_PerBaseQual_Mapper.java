package baseQual;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.SequencedFragment;
import utils.IdentifierPositionKeyWritable;
import utils.QualityCountHelperWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 04.02.16.
 */
public class FastQ_PerBaseQual_Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, SequencedFragment, IdentifierPositionKeyWritable, QualityCountHelperWritable> {
    private static final int OFFSET = 33;

    private Map<IdentifierPositionKeyWritable, QualityCountHelperWritable> keyMapping = new HashMap<IdentifierPositionKeyWritable, QualityCountHelperWritable>();

    @Override
    protected void map(Object key, SequencedFragment value, Context context) throws IOException, InterruptedException {
        final String sampleIdentifier = ((FileSplit)context.getInputSplit()).getPath().getName();

        final Text quality = value.getQuality();
        for (int i = 0; i < quality.getLength(); i++) {
            IdentifierPositionKeyWritable outputKey = new IdentifierPositionKeyWritable(sampleIdentifier, (i + 1));
            int qualValue = getCorrespondingIntValue(quality.charAt(i));

            if(!this.keyMapping.containsKey(outputKey)) {
                this.keyMapping.put(outputKey, new QualityCountHelperWritable(qualValue));
            } else {
                this.keyMapping.get(outputKey).addQualityValueToSum(qualValue);
                this.keyMapping.get(outputKey).incrementNumberOfQualityValues();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator it = this.keyMapping.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            context.write((IdentifierPositionKeyWritable) pair.getKey(), (QualityCountHelperWritable) pair.getValue());
        }
    }

    private int getCorrespondingIntValue(int qualVal) {
        return qualVal - FastQ_PerBaseQual_Mapper.OFFSET;
    }

}