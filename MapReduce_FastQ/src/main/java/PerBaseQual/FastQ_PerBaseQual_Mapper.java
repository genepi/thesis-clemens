package PerBaseQual;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.seqdoop.hadoop_bam.SequencedFragment;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 06.08.15.
 */
public class FastQ_PerBaseQual_Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, SequencedFragment, IntWritable, DoubleWritable> {
    private static final int OFFSET = 33;

    @Override
    protected void map(Object key, SequencedFragment value, Context context) throws IOException, InterruptedException {
        final Text quality = value.getQuality();
        for (int i = 0; i < quality.getLength(); i++) {
            context.write(new IntWritable(i + 1), new DoubleWritable(getCorrespondingIntValue(quality.charAt(i))));
        }
    }

    private int getCorrespondingIntValue(int qualVal) {
        return qualVal - FastQ_PerBaseQual_Mapper.OFFSET;
    }

}