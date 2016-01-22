package NaiveVariantCaller;

import org.apache.hadoop.mapreduce.Reducer;
import utils.NaiveVariantCallerKeyWritable;
import utils.NaiveVariantCallerPositionWritable;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 22.01.16.
 */
public class NaiveVariantCaller_Combiner extends org.apache.hadoop.mapreduce.Reducer<NaiveVariantCallerKeyWritable, NaiveVariantCallerPositionWritable, NaiveVariantCallerKeyWritable, NaiveVariantCallerPositionWritable> {

    @Override
    protected void reduce(NaiveVariantCallerKeyWritable key, Iterable<NaiveVariantCallerPositionWritable> values, Context context) throws IOException, InterruptedException {
        int totalNoOf_BASE_A = 0;
        int totalNoOf_BASE_C = 0;
        int totalNoOf_BASE_G = 0;
        int totalNoOf_BASE_T = 0;

        for (NaiveVariantCallerPositionWritable record : values) {
            totalNoOf_BASE_A += record.getNumberOfBase_A();
            totalNoOf_BASE_C += record.getNumberOfBase_C();
            totalNoOf_BASE_G += record.getNumberOfBase_G();
            totalNoOf_BASE_T += record.getNumberOfBase_T();
        }

        context.write(key, new NaiveVariantCallerPositionWritable(totalNoOf_BASE_A, totalNoOf_BASE_C, totalNoOf_BASE_G, totalNoOf_BASE_T));
    }

}
