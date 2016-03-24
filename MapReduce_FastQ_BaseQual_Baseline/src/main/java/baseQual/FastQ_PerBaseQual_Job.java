package baseQual;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import utils.IdentifierPositionKeyWritable;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 04.02.16.
 */
public class FastQ_PerBaseQual_Job {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.reduce.java.opts", "-Xmx6500M");
        Job job = Job.getInstance(conf, "MR_FastQ_PerBaseQual_Baseline");

        job.setJarByClass(FastQ_PerBaseQual_Job.class);
        job.setMapperClass(FastQ_PerBaseQual_Mapper.class);
        job.setReducerClass(FastQ_PerBaseQual_Reducer.class);

        job.setInputFormatClass(FastqInputFormat.class);
        job.setMapOutputKeyClass(IdentifierPositionKeyWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IdentifierPositionKeyWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(FastQ_PerBaseQual_OutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true)) {
            System.err.println("sort :: Job failed.");
        }
    }

}
