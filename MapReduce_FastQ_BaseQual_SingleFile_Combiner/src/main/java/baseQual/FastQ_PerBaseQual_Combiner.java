package baseQual;

import org.apache.hadoop.io.IntWritable;
import utils.QualityCountHelperWritable;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */
public class FastQ_PerBaseQual_Combiner extends org.apache.hadoop.mapreduce.Reducer<IntWritable, QualityCountHelperWritable, IntWritable, QualityCountHelperWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<QualityCountHelperWritable> values, Context context) throws IOException, InterruptedException {
        int sumOfQualityValues = 0;
        int numberOfQualityValues = 0;
        for (QualityCountHelperWritable qualCounter : values) {
            sumOfQualityValues += qualCounter.getSumOfQualityValues();
            numberOfQualityValues += qualCounter.getNumberOfQualityValues();
        }
        context.write(key, new QualityCountHelperWritable(sumOfQualityValues, numberOfQualityValues));
    }

}