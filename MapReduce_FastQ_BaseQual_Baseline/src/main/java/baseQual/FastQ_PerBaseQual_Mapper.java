package baseQual;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.SequencedFragment;
import utils.IdentifierPositionKeyWritable;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */
public class FastQ_PerBaseQual_Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, SequencedFragment, IdentifierPositionKeyWritable, IntWritable> {
    private static final int OFFSET = 33;

    @Override
    protected void map(Object key, SequencedFragment value, Context context) throws IOException, InterruptedException {
        final String sampleIdentifier = ((FileSplit)context.getInputSplit()).getPath().getName();

        final Text quality = value.getQuality();
        for (int i = 0; i < quality.getLength(); i++) {
            context.write(new IdentifierPositionKeyWritable(sampleIdentifier, (i + 1)), new IntWritable(getCorrespondingIntValue(quality.charAt(i))));
        }
    }

    private int getCorrespondingIntValue(int qualVal) {
        return qualVal - FastQ_PerBaseQual_Mapper.OFFSET;
    }

}