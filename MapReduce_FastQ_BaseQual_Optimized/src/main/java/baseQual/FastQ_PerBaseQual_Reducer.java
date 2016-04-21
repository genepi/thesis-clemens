package baseQual;

import org.apache.hadoop.io.DoubleWritable;
import utils.IdentifierPositionKeyWritable;
import utils.QualityCountHelperWritable;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */
public class FastQ_PerBaseQual_Reducer extends org.apache.hadoop.mapreduce.Reducer<IdentifierPositionKeyWritable, QualityCountHelperWritable, IdentifierPositionKeyWritable, DoubleWritable> {

    @Override
    protected void reduce(IdentifierPositionKeyWritable key, Iterable<QualityCountHelperWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, getMeanValue(values));
    }

    private DoubleWritable getMeanValue(Iterable<QualityCountHelperWritable> values) {
        int sumOfQualityValues = 0;
        int numberOfQualityValues = 0;
        for (QualityCountHelperWritable qualCounter : values) {
            sumOfQualityValues += qualCounter.getSumOfQualityValues();
            numberOfQualityValues += qualCounter.getNumberOfQualityValues();
        }
        return new DoubleWritable((double)sumOfQualityValues / (double)numberOfQualityValues);
    }

}