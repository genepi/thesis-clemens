package baseQual;

import utils.IdentifierPositionKeyWritable;
import utils.QualityCountHelperWritable;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */
public class FastQ_PerBaseQual_Combiner extends org.apache.hadoop.mapreduce.Reducer<IdentifierPositionKeyWritable, QualityCountHelperWritable, IdentifierPositionKeyWritable, QualityCountHelperWritable> {

    @Override
    protected void reduce(IdentifierPositionKeyWritable key, Iterable<QualityCountHelperWritable> values, Context context) throws IOException, InterruptedException {
        int sumOfQualityValues = 0;
        int numberOfQualityValues = 0;
        for (QualityCountHelperWritable qualCounter : values) {
            sumOfQualityValues += qualCounter.getSumOfQualityValues();
            numberOfQualityValues += qualCounter.getNumberOfQualityValues();
        }
        context.write(key, new QualityCountHelperWritable(sumOfQualityValues, numberOfQualityValues));
    }

}