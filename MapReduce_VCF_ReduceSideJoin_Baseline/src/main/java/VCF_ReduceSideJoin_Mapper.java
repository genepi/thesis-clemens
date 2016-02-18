import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.io.LongWritable;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromPosKeyWritable;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 13.02.16.
 */
public class VCF_ReduceSideJoin_Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, VariantContextWritable, ChromPosKeyWritable, VariantContextWritable> {

    @Override
    protected void map(LongWritable key, VariantContextWritable value, Context context) throws IOException, InterruptedException {
        final VariantContext variantContext = value.get();
        int chromosome = Integer.valueOf(variantContext.getContig());
        int position = variantContext.getStart();
        context.write(new ChromPosKeyWritable(chromosome, position), value);
    }

}