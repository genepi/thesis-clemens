import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.io.Text;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromPositionOrderKey;

import java.io.IOException;
import java.util.Iterator;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 12.02.16.
 */
public class VCF_ReduceSideJoin_Reducer extends org.apache.hadoop.mapreduce.Reducer<ChromPositionOrderKey, VariantContextWritable, ChromPositionOrderKey, Text> {
    private final char delimiter = '\t';

    @Override
    protected void reduce(ChromPositionOrderKey key, Iterable<VariantContextWritable> values, Context context) throws IOException, InterruptedException {
        Iterator it = values.iterator();
        VariantContext smallFileVariantContext = ((VariantContextWritable)it.next()).get();
        if (!it.hasNext()) { // no join partner available
            return;
        }
        VariantContext bigFileVariantContext = ((VariantContextWritable)it.next()).get();


        //TODO join the two files
        //TODO first step alle "vorderen" Felder joinen und das Info-Feld des 2. Files (HRC.r1.....) setzen

        StringBuilder sb = new StringBuilder();
        sb.append(bigFileVariantContext.getID());
        sb.append(delimiter);
        sb.append(smallFileVariantContext.getReference());
        sb.append(delimiter);
        sb.append(smallFileVariantContext.getAlleles());
        sb.append(delimiter);
        sb.append(smallFileVariantContext.getPhredScaledQual());
        sb.append(delimiter);
        sb.append(smallFileVariantContext.getFilters());
//        sb.append(bigFileVariantContext.)

        //TODO get all fields into the stringbuilder (in the correct order)

        context.write(key, new Text(sb.toString()));
    }

}