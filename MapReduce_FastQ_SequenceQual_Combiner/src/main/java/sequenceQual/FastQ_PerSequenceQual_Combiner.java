package sequenceQual;

import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 04.02.16.
 */
public class FastQ_PerSequenceQual_Combiner extends org.apache.hadoop.mapreduce.Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }

}