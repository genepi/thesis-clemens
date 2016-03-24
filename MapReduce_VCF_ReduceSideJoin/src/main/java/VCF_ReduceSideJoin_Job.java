import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromOrderKeyWritable;
import util.JoinedResultWritable;
import util.NaturalKeyPartitioner;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 18.02.16.
 */
public class VCF_ReduceSideJoin_Job {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("usage: hadoop jar Mapreduce_VCF_ReduceSideJoin-1.0-SNAPSHOT.jar <sample vcf file> <reference vcf file> <output dir>");
            return;
        }

        Configuration conf = new Configuration();
        conf.set("leftRelation", args[0].substring(args[0].lastIndexOf('/')+1));
        Job job = Job.getInstance(conf, "MR_VCF_ReduceSideJoin");

        job.setJarByClass(VCF_ReduceSideJoin_Job.class);
        job.setMapperClass(VCF_ReduceSideJoin_Mapper.class);
        job.setReducerClass(VCF_ReduceSideJoin_Reducer.class);

        job.setPartitionerClass(NaturalKeyPartitioner.class);

        //TODO talk about this case
//        job.setSortComparatorClass(CompositeKeyComparator.class);
//        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);

        job.setInputFormatClass(VCFInputFormat.class);
        job.setMapOutputKeyClass(ChromOrderKeyWritable.class);
        job.setMapOutputValueClass(VariantContextWritable.class);

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
