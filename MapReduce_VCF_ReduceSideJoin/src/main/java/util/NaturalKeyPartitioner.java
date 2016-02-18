package util;

import org.apache.hadoop.mapreduce.Partitioner;
import org.seqdoop.hadoop_bam.VariantContextWritable;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 12.02.16.
 *
 * Uses the natural key to partition the data onto the reducers.
 */
public class NaturalKeyPartitioner extends Partitioner<ChromPositionOrderKey, VariantContextWritable> {

    @Override
    public int getPartition(ChromPositionOrderKey key, VariantContextWritable value, int numPartitions) {
        int hash = key.getChromosome();
        hash = 31 * hash + key.getPosition();
        return hash % numPartitions;
    }

}