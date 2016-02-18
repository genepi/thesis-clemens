import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.io.Text;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromPosKeyWritable;

import java.io.IOException;
import java.util.Iterator;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 13.02.16.
 */
public class VCF_ReduceSideJoin_Reducer extends org.apache.hadoop.mapreduce.Reducer<ChromPosKeyWritable, VariantContextWritable, ChromPosKeyWritable, Text>  {
    private final char delimiter = '\t';

    @Override
    protected void reduce(ChromPosKeyWritable key, Iterable<VariantContextWritable> values, Context context) throws IOException, InterruptedException {
        final Iterator<VariantContextWritable> it = values.iterator();
        VariantContext s1 = ((VariantContextWritable)it.next()).get();

        if (!it.hasNext()) {
            return;
        }

        VariantContext s2 = ((VariantContextWritable)it.next()).get();

        StringBuilder sb = new StringBuilder();
        sb.append(s2.getID());
//        sb.append(delimiter);


        //TODO build the output

        context.write(key, new Text(sb.toString()));
    }

}