package PerBaseSequenceContent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import utils.BaseSequenceContentWritable;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 06.08.15.
 */
public class FastQ_PerBaseSequenceContent_Job {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"FastQ_PerBaseSequenceContent_Job");

        job.setJarByClass(FastQ_PerBaseSequenceContent_Job.class);
        job.setMapperClass(FastQ_PerBaseSequenceContent_Mapper.class);
        job.setReducerClass(FastQ_PerBaseSequenceContent_Reducer.class);

        job.setInputFormatClass(FastqInputFormat.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BaseSequenceContentWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true)) {
            System.err.println("sort :: Job failed.");
        }
    }

}