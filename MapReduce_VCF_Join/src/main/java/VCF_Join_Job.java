import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import util.*;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */
public class VCF_Join_Job {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("usage: hadoop jar Mapreduce_VCF_Join-1.0-SNAPSHOT.jar <sample vcf file> <reference vcf file> <output dir>");
            return;
        }

        Configuration conf = new Configuration();
        conf.set("sample", args[0].substring(args[0].lastIndexOf('/')+1));
        conf.set("reference", args[1].substring(args[1].lastIndexOf('/')+1));
        Job job = Job.getInstance(conf, "MR_VCF_ReduceSideJoin");

        job.setJarByClass(VCF_Join_Job.class);
        job.setMapperClass(VCF_Join_Mapper.class);
        job.setReducerClass(VCF_Join_Reducer.class);

        //partitioning, grouping and secondary sort classes
        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);

        job.setInputFormatClass(VCFInputFormat.class);
        job.setMapOutputKeyClass(ChromPosKey.class);
        job.setMapOutputValueClass(JoinedResultWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(JoinedResultWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        if (!job.waitForCompletion(true)) {
            System.err.println("sort :: Job failed.");
        }
    }

}