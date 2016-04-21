package NaiveVariantCaller;

import utils.NaiveVariantCallerKeyWritable;
import utils.NaiveVariantCallerBaseRecordWritable;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */
public class NaiveVariantCaller_Combiner extends org.apache.hadoop.mapreduce.Reducer<NaiveVariantCallerKeyWritable, NaiveVariantCallerBaseRecordWritable, NaiveVariantCallerKeyWritable, NaiveVariantCallerBaseRecordWritable> {

    @Override
    protected void reduce(NaiveVariantCallerKeyWritable key, Iterable<NaiveVariantCallerBaseRecordWritable> values, Context context) throws IOException, InterruptedException {
        int totalNoOf_BASE_A = 0;
        int totalNoOf_BASE_C = 0;
        int totalNoOf_BASE_G = 0;
        int totalNoOf_BASE_T = 0;

        for (NaiveVariantCallerBaseRecordWritable record : values) {
            totalNoOf_BASE_A += record.getNoOf_BASE_A();
            totalNoOf_BASE_C += record.getNoOf_BASE_C();
            totalNoOf_BASE_G += record.getNoOf_BASE_G();
            totalNoOf_BASE_T += record.getNoOf_BASE_T();
        }
        context.write(key, new NaiveVariantCallerBaseRecordWritable(totalNoOf_BASE_A, totalNoOf_BASE_C, totalNoOf_BASE_G, totalNoOf_BASE_T));
    }
}
