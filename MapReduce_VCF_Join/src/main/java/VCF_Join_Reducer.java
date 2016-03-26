import htsjdk.tribble.util.ParsingUtils;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.io.NullWritable;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromPosKey;
import util.JoinedResultWritable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
                this.writeResultToContext(obj2, this.attributesToString(obj1.getAttributes()), context);
            } else if (obj1.hasGenotypes()) {
                this.writeResultToContext(obj1, " ", context);
            }
            obj1 = obj2;
            if (!it.hasNext() && obj1.hasGenotypes()) { //last element
                this.writeResultToContext(obj1, " ", context);
            }
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
        String filter = this.filtersToString(obj.getCommonInfo().getFilters());
        String info = this.attributesToString(obj.getAttributes());
        String genotypes = this.genotypesToString(obj.getGenotypes());

        context.write(null, new JoinedResultWritable(chrom, pos, id, alt, ref, qual, filter, info, genotypes, infoRef));
    }

    private String filtersToString(Set<String> filters) {
        if(filters.isEmpty()) { //samtools uses empty set for passes filters
            return "PASS";
        }
        char delimiter = ';';
        StringBuilder sb = new StringBuilder();
        for (String filter : filters) {
            sb.append(filter);
            sb.append(delimiter);
        }
        return sb.toString();
    }

    private String attributesToString(Map<String,Object> att) {
        char delimiter = ';';
        char equals = '=';
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<String,Object> entry : att.entrySet()) {
            sb.append(entry.getKey());
            sb.append(equals);
            sb.append(entry.getValue());
            sb.append(delimiter);
        }
        return sb.toString();
    }

    private String genotypesToString(GenotypesContext genotypes) {

        //TODO handle genotype information

        return "genotypes ...";
    }

}