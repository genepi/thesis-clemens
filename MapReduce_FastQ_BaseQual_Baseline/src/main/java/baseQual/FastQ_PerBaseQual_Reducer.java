package baseQual;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import utils.IdentifierPositionKeyWritable;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */
public class FastQ_PerBaseQual_Reducer extends org.apache.hadoop.mapreduce.Reducer<IdentifierPositionKeyWritable, IntWritable, IdentifierPositionKeyWritable, DoubleWritable> {

    @Override
    protected void reduce(IdentifierPositionKeyWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, getMeanValue(values));
    }

    private DoubleWritable getMeanValue(Iterable<IntWritable> values) {
        int numberOfQualityValues = 0;
        double sumOfQualityValues = 0;
        for (IntWritable qualVal : values) {
            sumOfQualityValues += qualVal.get();
            numberOfQualityValues++;
        }
        return new DoubleWritable(sumOfQualityValues / numberOfQualityValues);
    }

}