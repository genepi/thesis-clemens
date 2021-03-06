package util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 *
 * Groups values based on the natural key (chrom)
 */
public class NaturalKeyGroupingComparator extends WritableComparator {

    protected NaturalKeyGroupingComparator() {
        super(ChromPosKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        ChromPosKey k1 = (ChromPosKey) w1;
        ChromPosKey k2 = (ChromPosKey) w2;

        int result = (k1.getChromosome() < k2.getChromosome() ? -1 : (k1.getChromosome() == k2.getChromosome() ? 0 : 1));
        return result;
    }

}