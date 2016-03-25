import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromPosKey;

import java.io.IOException;
import java.util.Iterator;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 25.03.16.
 */
public class VCF_Join_Reducer extends org.apache.hadoop.mapreduce.Reducer<ChromPosKey, VariantContextWritable, NullWritable, Text> {

    @Override
    protected void reduce(ChromPosKey key, Iterable<VariantContextWritable> values, Context context) throws IOException, InterruptedException {
        final Iterator<VariantContextWritable> it = values.iterator();

        while (it.hasNext()) {
            context.write(null, new Text(key.toString() + it.next().get().toString()));
        }
    }

}