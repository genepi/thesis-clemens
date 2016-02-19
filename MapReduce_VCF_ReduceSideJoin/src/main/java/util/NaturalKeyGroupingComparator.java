package util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 18.02.16.
 */
public class NaturalKeyGroupingComparator extends WritableComparator {

    protected NaturalKeyGroupingComparator() {
        super(ChromOrderKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        ChromOrderKeyWritable k1 = (ChromOrderKeyWritable)w1;
        ChromOrderKeyWritable k2 = (ChromOrderKeyWritable)w2;

        int k1Chrom = k1.getChromosome();
        int k1Pos = k1.getPosition();
        int k2Chrom = k2.getChromosome();
        int k2Pos = k2.getPosition();

        int result = (k1Chrom < k2Chrom ? -1 : (k1Chrom == k2Chrom ? 0 : 1));
        if (0 == result) {
            result = (k1Pos < k2Pos ? -1 : (k1Pos == k2Pos ? 0 : 1));
        }
        return result;
    }

}