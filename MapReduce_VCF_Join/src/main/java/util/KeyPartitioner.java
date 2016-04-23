package util;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 *
 * partitions key based on "part" of natural key (chrom)
 */
public class KeyPartitioner extends Partitioner<ChromPosKey, JoinedResultWritable> {

    @Override
    public int getPartition(ChromPosKey chromPosKey, JoinedResultWritable val, int numPartitions) {
        int hash = chromPosKey.getChromosome();
        return hash % numPartitions;
    }

}