package util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 12.02.16.
 *
 * Used for the secondary sorting process. Takes natural key and joinOrder into account.
 */
public class CompositeKeyComparator extends WritableComparator {

    protected CompositeKeyComparator() {
        super(ChromPositionOrderKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        ChromPositionOrderKey k1 = (ChromPositionOrderKey)w1;
        ChromPositionOrderKey k2 = (ChromPositionOrderKey)w2;

        int result = 0;
        if (k1.getChromosome() != k2.getChromosome()) {
            result = (k1.getChromosome() < k2.getChromosome() ? -1 : 1);
        } else if (k1.getPosition() != k2.getPosition()) {
            result = (k1.getPosition() < k2.getPosition() ? -1 : 1);
        }
        if (0 == result) {
            result = (k1.getJoinOrder() < k2.getJoinOrder() ? -1 : (k1.getJoinOrder() == k2.getJoinOrder() ? 0 : 1));
        }
        return result;
    }

}