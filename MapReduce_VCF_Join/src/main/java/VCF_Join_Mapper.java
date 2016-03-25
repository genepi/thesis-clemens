import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromPosKey;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 25.03.16.
 */
public class VCF_Join_Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, VariantContextWritable, ChromPosKey, VariantContextWritable> {

    @Override
    protected void map(LongWritable key, VariantContextWritable value, Context context) throws IOException, InterruptedException {
        final String sampleIdentifier = ((FileSplit)context.getInputSplit()).getPath().getName();
        int orderValue = (sampleIdentifier.equals(context.getConfiguration().get("reference"))) ? 0 : 1;

        final VariantContext variantContext = value.get();
        int chrom = Integer.valueOf(variantContext.getContig());
        int pos = variantContext.getStart();

        context.write(new ChromPosKey(chrom, pos, orderValue), value);
    }

}