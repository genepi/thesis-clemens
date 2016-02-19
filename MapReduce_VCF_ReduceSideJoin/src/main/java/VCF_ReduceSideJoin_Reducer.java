import htsjdk.tribble.util.ParsingUtils;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.io.NullWritable;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromOrderKeyWritable;
import util.JoinedResultWritable;

import java.io.IOException;
import java.util.Iterator;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 18.02.16.
 */
public class VCF_ReduceSideJoin_Reducer extends org.apache.hadoop.mapreduce.Reducer<ChromOrderKeyWritable, VariantContextWritable, NullWritable, JoinedResultWritable> {

    @Override
    protected void reduce(ChromOrderKeyWritable key, Iterable<VariantContextWritable> values, Context context) throws IOException, InterruptedException {
        Iterator it = values.iterator();
        VariantContext previousVCFContext = null;
        VariantContext currentVcfContext;

        if (it.hasNext()) {
            previousVCFContext = ((VariantContextWritable)it.next()).get();
        }

        while (it.hasNext()) {
            currentVcfContext = ((VariantContextWritable)it.next()).get();

            if (previousVCFContext.getStart() == currentVcfContext.getStart()) { // join match
                final JoinedResultWritable res = new JoinedResultWritable();
                res.setChrom(Integer.valueOf(previousVCFContext.getContig()));
                res.setPos(previousVCFContext.getStart());

                if (previousVCFContext.hasID()) {
                    res.setId(previousVCFContext.getID());
                } else if (currentVcfContext.hasID()) {
                    res.setId(currentVcfContext.getID());
                }

                //TODO set the correct values for (ref, alt, qual, filter, attributes)

                res.setRef(previousVCFContext.getReference().toString().charAt(0));
                res.setAlt(previousVCFContext.getAltAlleleWithHighestAlleleCount().toString().charAt(0));
                res.setQual(0);
                res.setFilter(previousVCFContext.PASSES_FILTERS.toString());
                res.setInfo(ParsingUtils.sortedString(currentVcfContext.getAttributes()));

                context.write(null, res);
            }
            previousVCFContext = currentVcfContext;
        }
    }

}