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

        //TODO dont use magic-values -> put value into setupMethod

        final String sampleIdentifier = ((FileSplit)context.getInputSplit()).getPath().getName();
        int orderVal = (sampleIdentifier == "20mini.vcf") ? 0 : 1;

        VariantContext variantContext = value.get();
        context.write(new ChromOrderKeyWritable(Integer.valueOf(variantContext.getContig()), variantContext.getStart(), orderVal), value);
    }

}