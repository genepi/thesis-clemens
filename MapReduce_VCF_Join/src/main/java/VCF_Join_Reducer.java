import htsjdk.tribble.util.ParsingUtils;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.io.NullWritable;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromPosKey;
import util.JoinedResultWritable;

import java.io.IOException;
import java.io.InterruptedIOException;
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
                this.writeResultToContext(obj2, ParsingUtils.sortedString(obj1.getAttributes()), context);
            } else if (obj1.hasGenotypes()) {
                this.writeResultToContext(obj1, "", context);
            }
            obj1 = obj2;
        }
    }

    private void writeResultToContext(VariantContext obj, String infoRef, Context context) throws IOException, InterruptedException {
        final List<Allele> alleles = ParsingUtils.sortList(obj.getAlleles());
        int chrom = Integer.valueOf(obj.getContig());
        int pos = obj.getStart();
        String id = (obj.hasID() ? obj.getID() : ".");
        char alt = alleles.get(0).toString().charAt(0);
        char ref = alleles.get(1).toString().charAt(0);
        String qual = obj.hasLog10PError() ? String.valueOf(obj.getPhredScaledQual()) : ".";
        String filter = obj.PASSES_FILTERS.toString();
        String info = ParsingUtils.sortedString(obj.getAttributes());
//                String genotypes = obj2.getGenotypes().toString();
        String genotypes = "genotypes...";

        context.write(null, new JoinedResultWritable(chrom, pos, id, alt, ref, qual, filter, info, genotypes, infoRef));
    }

}