import htsjdk.tribble.util.ParsingUtils;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.io.NullWritable;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromPosKey;
import util.JoinedResultWritable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 25.03.16.
 */
public class VCF_Join_Reducer extends org.apache.hadoop.mapreduce.Reducer<ChromPosKey, VariantContextWritable, NullWritable, JoinedResultWritable> {

    @Override
    protected void reduce(ChromPosKey key, Iterable<VariantContextWritable> values, Context context) throws IOException, InterruptedException {
        final Iterator<VariantContextWritable> it = values.iterator();

        VariantContext obj1 = null;
        if (it.hasNext()) {
            obj1 = it.next().get();
        }

        while (it.hasNext()) {
            final VariantContext obj2 = it.next().get();
            if (obj1.getContig().equals(obj2.getContig()) && obj1.getStart() == obj2.getStart()) {

                final List<Allele> alleles = ParsingUtils.sortList(obj2.getAlleles());
                int chrom = Integer.valueOf(obj2.getContig());
                int pos = obj2.getStart();
                String id = (obj2.hasID() ? obj2.getID() : ".");
                char alt = alleles.get(0).toString().charAt(0);
                char ref = alleles.get(1).toString().charAt(0);
                String qual = obj2.hasLog10PError() ? String.valueOf(obj2.getPhredScaledQual()) : ".";
                String filter = obj2.PASSES_FILTERS.toString();
                String info = ParsingUtils.sortedString(obj2.getAttributes());
//                String genotypes = obj2.getGenotypes().toString();
                String genotypes = "genotypes...";
                String infoRef = ParsingUtils.sortedString(obj1.getAttributes());

                context.write(null, new JoinedResultWritable(chrom, pos, id, alt, ref, qual, filter, info, genotypes, infoRef));

            } else {
                obj1 = obj2;
            }
        }
    }

}