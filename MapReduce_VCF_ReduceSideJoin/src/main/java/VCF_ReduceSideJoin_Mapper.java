import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromOrderKeyWritable;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 18.02.16.
 */
public class VCF_ReduceSideJoin_Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, VariantContextWritable, ChromOrderKeyWritable, VariantContextWritable> {

    @Override
    protected void map(LongWritable key, VariantContextWritable value, Context context) throws IOException, InterruptedException {
        final String sampleIdentifier = ((FileSplit)context.getInputSplit()).getPath().getName();
        int orderVal = (sampleIdentifier.equals(context.getConfiguration().get("leftRelation"))) ? 0 : 1;

        System.out.println(context.getConfiguration().get("leftRelation"));


        VariantContext variantContext = value.get();
        context.write(new ChromOrderKeyWritable(Integer.valueOf(variantContext.getContig()), variantContext.getStart(), orderVal), value);
    }

}