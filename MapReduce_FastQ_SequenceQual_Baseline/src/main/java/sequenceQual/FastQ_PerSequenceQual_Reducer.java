package sequenceQual;

import org.apache.hadoop.io.IntWritable;
import utils.IdentifierPositionKeyWritable;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 04.02.16.
 */
public class FastQ_PerSequenceQual_Reducer extends org.apache.hadoop.mapreduce.Reducer<IdentifierPositionKeyWritable, IntWritable, IdentifierPositionKeyWritable, IntWritable> {

    @Override
    public void reduce(IdentifierPositionKeyWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }

}