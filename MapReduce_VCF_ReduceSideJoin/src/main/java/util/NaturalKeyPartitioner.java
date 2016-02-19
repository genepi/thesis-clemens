package util;

import org.seqdoop.hadoop_bam.VariantContextWritable;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 18.02.16.
 */
public class NaturalKeyPartitioner extends org.apache.hadoop.mapreduce.Partitioner<ChromOrderKeyWritable, VariantContextWritable> {

    @Override
    public int getPartition(ChromOrderKeyWritable key, VariantContextWritable value, int numPartitions) {
        return key.getChromosome() % numPartitions;
    }

}