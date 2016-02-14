import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import util.JoinedResultWritable;

import java.net.URI;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 14.02.16.
 */
public class VCF_MapSideJoin_Job extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),
                new VCF_MapSideJoin_Job(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        Configuration conf = job.getConfiguration();
        job.setJobName("MR_VCF_ReduceSideJoin_Baseline");
        DistributedCache.addCacheFile(new URI("/user/cloudera/input/vcftest/20.filtered.PASS_noHeader.vcf"), conf);

        job.setJarByClass(VCF_MapSideJoin_Job.class);
        job.setMapperClass(VCF_MapSideJoin_Mapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(VCFInputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(JoinedResultWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(JoinedResultWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}