package sequenceQual;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.SequencedFragment;
import utils.IdentifierPositionKeyWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */
public class FastQ_PerSequenceQual_Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, SequencedFragment, IdentifierPositionKeyWritable, IntWritable> {
    private static final int OFFSET = 33;

    private Map<IdentifierPositionKeyWritable, Integer> keyMapping = new HashMap<IdentifierPositionKeyWritable, Integer>();

    @Override
    protected void map(Object key, SequencedFragment value, Context context) throws IOException, InterruptedException {
        final String sampleIdentifier = ((FileSplit)context.getInputSplit()).getPath().getName();

        IdentifierPositionKeyWritable outputKey = new IdentifierPositionKeyWritable(sampleIdentifier, getMeanValue(value.getQuality()));
        if(!this.keyMapping.containsKey(outputKey)) {
            this.keyMapping.put(outputKey, 1);
        } else {
            this.keyMapping.put(outputKey, this.keyMapping.get(outputKey) + 1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator it = this.keyMapping.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            context.write((IdentifierPositionKeyWritable) pair.getKey(), new IntWritable((Integer) pair.getValue()));
        }
    }

    private Integer getMeanValue(Text quality) {
        int sumOfQualityValues = 0;
        for(Character qualVal : quality.toString().toCharArray()) {
            sumOfQualityValues += getCorrespondingIntValue(qualVal);
        }
        return (int)(Math.round((double)sumOfQualityValues/(double)quality.getLength()));
    }

    private int getCorrespondingIntValue(Character qualVal) {
        int intQualVal = (int)qualVal;
        return intQualVal-FastQ_PerSequenceQual_Mapper.OFFSET;
    }

}