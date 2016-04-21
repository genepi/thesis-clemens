import htsjdk.tribble.util.ParsingUtils;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromPosKey;
import util.JoinedResultWritable;

import java.io.IOException;
import java.util.*;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */
public class VCF_Join_Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, VariantContextWritable, ChromPosKey, JoinedResultWritable> {

    @Override
    protected void map(LongWritable key, VariantContextWritable value, Context context) throws IOException, InterruptedException {
        final String sampleIdentifier = ((FileSplit)context.getInputSplit()).getPath().getName();
        int orderValue = (sampleIdentifier.equals(context.getConfiguration().get("sample"))) ? 0 : 1;
        final VariantContext variantContext = value.get();
        int chrom = Integer.valueOf(variantContext.getContig());
        int pos = variantContext.getStart();
        context.write(new ChromPosKey(chrom, pos, orderValue), constructWritableObj(orderValue, variantContext));
    }

    private JoinedResultWritable constructWritableObj(int orderValue, VariantContext value) {
        final List<Allele> alleles = ParsingUtils.sortList(value.getAlleles());
        final List<Allele> alternateAlleles = value.getAlternateAlleles();

        int chrom = Integer.valueOf(value.getContig());
        int pos = value.getStart();
        String id = (value.hasID() ? value.getID() : ".");
        String ref = alleles.get(0).getBaseString();
        String alt = combineAlleles(alternateAlleles);
        String qual = value.hasLog10PError() ? String.valueOf(value.getPhredScaledQual()) : ".";
        String filter = this.filtersToString(value.getCommonInfo().getFilters());
        String info = this.attributesToSortedString(value.getAttributes());
        String format = "GT";
        String genotypes = "";
        if (value.hasGenotypes()) {
            genotypes = this.genotypesToString(value.getGenotypes(), ref, alternateAlleles);
        }
        return new JoinedResultWritable(orderValue, chrom, pos, id, ref, alt, qual, filter, info, format, genotypes);
    }

    private String combineAlleles(List<Allele> alleles) {
        StringBuilder ret = new StringBuilder(alleles.get(0).getBaseString());
        for (int i = 1; i < alleles.size(); ++i) {
            ret.append(",");
            ret.append(alleles.get(i).getBaseString());
        }
        return ret.toString();
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

    private <T extends Comparable<T>, V> String attributesToSortedString(Map<T, V> att) {
        List<T> t = new ArrayList<T>(att.keySet());
        Collections.sort(t);
        List<String> pairs = new ArrayList<String>();
        for (T k : t) {
            pairs.add(k + "=" + att.get(k));
        }
        String[] strings = pairs.toArray(new String[pairs.size()]);
        return this.join(";", strings, 0, strings.length);
    }

    private String join(String separator, String[] strings, int start, int end) {
        if ((end - start) == 0) {
            return "";
        }
        StringBuilder ret = new StringBuilder(strings[start]);
        for (int i = start + 1; i < end; ++i) {
            ret.append(separator);
            ret.append(strings[i]);
        }
        return ret.toString();
    }

    private String genotypesToString(GenotypesContext genotypes, String ref, List<Allele> altAlleles) {
        StringBuilder sb = new StringBuilder();
        char delimiter = '\t';
        for (Genotype genotype : genotypes) {
            if (genotype.getPloidy() == 0) {
                sb.append("NA");
            }
            final String separator = genotype.isPhased() ? genotype.PHASED_ALLELE_SEPARATOR : genotype.UNPHASED_ALLELE_SEPARATOR;

            final List<String> al = new ArrayList<String>(genotype.getPloidy());
            String base;
            for ( Allele a : genotype.getAlleles() ) {
                base = a.getBaseString();
                if (base.equals(ref)) { //matches reference
                    al.add(String.valueOf(0));
                } else { //is an alternative allele
                    for(int i = 0; i < altAlleles.size(); i++) {
                        if (base.equals(altAlleles.get(i).getBaseString())) {
                            al.add(String.valueOf(i+1));
                            break;
                        }
                    }
                }
            }

            sb.append(ParsingUtils.join(separator, al));
            sb.append(delimiter);
        }
        return sb.toString();
    }

}