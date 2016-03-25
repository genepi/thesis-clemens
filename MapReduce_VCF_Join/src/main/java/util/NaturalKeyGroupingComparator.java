package util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 25.03.16.
 *
 * Groups values based on the natural key (chrom,pos)
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
        if (0 == result) {
            result = (k1.getPosition() < k2.getPosition() ? -1 : (k1.getPosition() == k2.getPosition() ? 0 : 1));
        }
        return result;
    }

}