import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromPositionOrderKey;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 12.02.16.
 */
public class VCF_ReduceSideJoin_Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, VariantContextWritable, ChromPositionOrderKey, VariantContextWritable> {

    @Override
    protected void map(LongWritable key, VariantContextWritable value, Context context) throws IOException, InterruptedException {
        final String sampleIdentifier = ((FileSplit)context.getInputSplit()).getPath().getName();
        VariantContext variantContext = value.get();

        int chromosome = Integer.valueOf(variantContext.getContig());
        int position = variantContext.getStart();
        int joinOrder = Integer.valueOf(context.getConfiguration().get(sampleIdentifier));

        context.write(new ChromPositionOrderKey(chromosome, position, joinOrder), value);
    }

}