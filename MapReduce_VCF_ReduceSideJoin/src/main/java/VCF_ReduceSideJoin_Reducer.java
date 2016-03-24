import htsjdk.tribble.util.ParsingUtils;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.io.NullWritable;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromOrderKeyWritable;
import util.JoinedResultWritable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 18.02.16.
 */
public class VCF_ReduceSideJoin_Reducer extends org.apache.hadoop.mapreduce.Reducer<ChromOrderKeyWritable, VariantContextWritable, NullWritable, JoinedResultWritable> {

    @Override
    protected void reduce(ChromOrderKeyWritable key, Iterable<VariantContextWritable> values, Context context) throws IOException, InterruptedException {
        final Iterator<VariantContextWritable> it = values.iterator();

        while (it.hasNext()) {

            VariantContext sample = it.next().get();
            if (0 == context.getCurrentKey().getOrderVal()) {

                //TODO vom nächsten Dings checken, ob es einen match gibt und die Info-Spalte rausholen...

                System.out.println("blub");

                //TODO hier scheint es kein "next" zu geben... ??
                //TODO werden die Werte nicht auf den gleichen Reducer geschickt?
                //TODO es gibt aber ja nur einen...


                if (it.hasNext()) {
                    VariantContext ref = it.next().get();
                    System.out.println("sample: " + sample.getStart() + ", ref: " + ref.getStart());

//                    if (ref.getStart() == sample.getStart()) {
//                        System.out.println("you should join here");
//                    } else {
//                        continue;
//                    }
                }

                //TODO join is missing at the moment... ;-)

                JoinedResultWritable res = new JoinedResultWritable();
                res.setChrom(Integer.valueOf(sample.getContig()));
                res.setPos(sample.getStart());
                res.setId((sample.hasID() ? sample.getID() : "."));

                final List<Allele> alleles = ParsingUtils.sortList(sample.getAlleles());
                res.setAlt(alleles.get(0).toString().charAt(0));
                res.setRef(alleles.get(1).toString().charAt(0));
                res.setQual(sample.hasLog10PError() ? String.valueOf(sample.getPhredScaledQual()) : ".");

                res.setFilter(sample.PASSES_FILTERS.toString());
                res.setId(ParsingUtils.sortedString(sample.getAttributes()));

                context.write(null, res);
            }

        }





        //TODO @Secondary Sorting
        //brauche ich den NaturalKeyGroupingComparator überhaupt. Wenn ich ihn habe, dann ist im Reducer nur 1x
        //der Wert, selbst wenn ich ihn in beiden input-Files drin habe...


    }

}