package util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 12.02.16.
 *
 * "Groups" values together according to the natural key. Ensures that K,V go to same reducers.
 */
public class NaturalKeyGroupingComparator extends WritableComparator {

    protected NaturalKeyGroupingComparator() {
        super(ChromPositionOrderKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        ChromPositionOrderKey k1 = (ChromPositionOrderKey)w1;
        ChromPositionOrderKey k2 = (ChromPositionOrderKey)w2;

        if (k1.getChromosome() != k2.getChromosome()) {
            return (k1.getChromosome() < k2.getChromosome() ? -1 : 1);
        } else if (k1.getPosition() != k2.getPosition()) {
            return (k1.getPosition() < k2.getPosition() ? -1 : 1);
        }
        return 0;
    }

}