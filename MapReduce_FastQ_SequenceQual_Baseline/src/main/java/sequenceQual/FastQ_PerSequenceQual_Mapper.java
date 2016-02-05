package sequenceQual;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.SequencedFragment;
import utils.IdentifierPositionKeyWritable;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 04.02.16.
 */
public class FastQ_PerSequenceQual_Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, SequencedFragment, IdentifierPositionKeyWritable, IntWritable> {
    private static final int OFFSET = 33;

    @Override
    protected void map(Object key, SequencedFragment value, Context context) throws IOException, InterruptedException {
        final String sampleIdentifier = ((FileSplit)context.getInputSplit()).getPath().getName();
        context.write(new IdentifierPositionKeyWritable(sampleIdentifier, getMeanValue(value.getQuality())), new IntWritable(1));
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