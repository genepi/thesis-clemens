package util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 25.03.16.
 *
 * partitions key based on "part" of natural key (chrom)
 */
public class KeyPartitioner extends Partitioner<ChromPosKey, DoubleWritable> {

    @Override
    public int getPartition(ChromPosKey chromPosKey, DoubleWritable val, int numPartitions) {
        int hash = chromPosKey.getChromosome();
        return hash % numPartitions;
    }

}