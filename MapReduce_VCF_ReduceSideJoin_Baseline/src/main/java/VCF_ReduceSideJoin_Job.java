import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromPosKeyWritable;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 13.02.16.
 */
public class VCF_ReduceSideJoin_Job {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MR_VCF_ReduceSideJoin_Baseline");

        job.setJarByClass(VCF_ReduceSideJoin_Job.class);
        job.setMapperClass(VCF_ReduceSideJoin_Mapper.class);
        job.setReducerClass(VCF_ReduceSideJoin_Reducer.class);

        job.setInputFormatClass(VCFInputFormat.class);
        job.setMapOutputKeyClass(ChromPosKeyWritable.class);
        job.setMapOutputValueClass(VariantContextWritable.class);

        job.setOutputKeyClass(ChromPosKeyWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true)) {
            System.err.println("sort :: Job failed.");
        }
    }
}
