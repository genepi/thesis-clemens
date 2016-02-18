package PerBaseQual;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 06.08.15.
 */
public class FastQ_PerBaseQual_Reducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, getMeanValue(values));
    }

    private DoubleWritable getMeanValue(Iterable<DoubleWritable> values) {
        double sumOfQualityValues = 0;
        double numberOfQualityValues = 0;
        for (DoubleWritable qualVal : values) {
            sumOfQualityValues += qualVal.get();
            numberOfQualityValues++;
        }
        return new DoubleWritable(sumOfQualityValues / numberOfQualityValues);
    }

}