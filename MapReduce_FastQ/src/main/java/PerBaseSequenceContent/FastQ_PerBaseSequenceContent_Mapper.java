package PerBaseSequenceContent;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.seqdoop.hadoop_bam.SequencedFragment;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 06.08.15.
 */
public class FastQ_PerBaseSequenceContent_Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, SequencedFragment, IntWritable, Text> {
    private static final char BASE_A = 'A';
    private static final char BASE_C = 'C';
    private static final char BASE_G = 'G';
    private static final char BASE_T = 'T';

    @Override
    protected void map(Object key, SequencedFragment value, Context context) throws IOException, InterruptedException {
        final Text sequence = value.getSequence();
        for (int i = 0; i < sequence.getLength(); i++) {
            IntWritable outputKey = new IntWritable(i+1);
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
                    break;
            }
        }
    }

}