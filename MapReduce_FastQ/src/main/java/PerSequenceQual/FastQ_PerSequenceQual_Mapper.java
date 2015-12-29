package PerSequenceQual;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.seqdoop.hadoop_bam.SequencedFragment;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 06.08.15.
 */
public class FastQ_PerSequenceQual_Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, SequencedFragment, IntWritable, IntWritable> {
    private static final int OFFSET = 33;

    @Override
    protected void map(Object key, SequencedFragment value, Context context) throws IOException, InterruptedException {
        context.write(getMeanValue(value.getQuality()), new IntWritable(1));
    }

    private IntWritable getMeanValue(Text quality) {
        int sumOfQualityValues = 0;
        for(Character qualVal : quality.toString().toCharArray()) {
            sumOfQualityValues += getCorrespondingIntValue(qualVal);
        }
        return new IntWritable(sumOfQualityValues/quality.getLength());
    }

    private int getCorrespondingIntValue(Character qualVal) {
        int intQualVal = (int)qualVal;
        return intQualVal-FastQ_PerSequenceQual_Mapper.OFFSET;
    }

}