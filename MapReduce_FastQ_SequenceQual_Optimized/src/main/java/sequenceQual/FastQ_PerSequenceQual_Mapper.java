package sequenceQual;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.seqdoop.hadoop_bam.SequencedFragment;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 04.02.16.
 */
public class FastQ_PerSequenceQual_Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, SequencedFragment, IntWritable, IntWritable> {
    private static final int OFFSET = 33;

    private Map<IntWritable, Integer> keyMapping = new HashMap<IntWritable, Integer>();

    @Override
    protected void map(Object key, SequencedFragment value, Context context) throws IOException, InterruptedException {
        IntWritable outputKey = getMeanValue(value.getQuality());
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
            context.write((IntWritable) pair.getKey(), new IntWritable((Integer) pair.getValue()));
        }
    }

    private IntWritable getMeanValue(Text quality) {
        int sumOfQualityValues = 0;
        for(Character qualVal : quality.toString().toCharArray()) {
            sumOfQualityValues += getCorrespondingIntValue(qualVal);
        }
        return new IntWritable(
                (int)(Math.round((double)sumOfQualityValues/(double)quality.getLength()))
        );
    }

    private int getCorrespondingIntValue(Character qualVal) {
        int intQualVal = (int)qualVal;
        return intQualVal-FastQ_PerSequenceQual_Mapper.OFFSET;
    }

}