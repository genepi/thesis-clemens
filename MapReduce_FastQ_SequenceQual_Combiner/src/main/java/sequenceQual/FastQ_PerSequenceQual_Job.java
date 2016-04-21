package sequenceQual;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import utils.IdentifierPositionKeyWritable;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */

public class FastQ_PerSequenceQual_Job {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MR_FastQ_PerSequenceQual_Combiner");

        job.setJarByClass(FastQ_PerSequenceQual_Job.class);
        job.setMapperClass(FastQ_PerSequenceQual_Mapper.class);
        job.setCombinerClass(FastQ_PerSequenceQual_Combiner.class);
        job.setReducerClass(FastQ_PerSequenceQual_Reducer.class);

        job.setInputFormatClass(FastqInputFormat.class);
        job.setMapOutputKeyClass(IdentifierPositionKeyWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IdentifierPositionKeyWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(FastQ_PerSequenceQual_OutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true)) {
            System.err.println("sort :: Job failed.");
        }
    }

}
