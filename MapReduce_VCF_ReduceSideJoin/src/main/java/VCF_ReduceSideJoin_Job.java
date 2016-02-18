import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromPositionOrderKey;
import util.CompositeKeyComparator;
import util.NaturalKeyGroupingComparator;
import util.NaturalKeyPartitioner;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 12.02.16.
 */
public class VCF_ReduceSideJoin_Job {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("20.filtered.PASS.vcf", "0");
        conf.set("HRC.r1.GRCh37.autosomes.mac5.sites_PART.vcf", "1");

        Job job = Job.getInstance(conf, "MR_VCF_ReduceSideJoin");

        job.setJarByClass(VCF_ReduceSideJoin_Job.class);
        job.setMapperClass(VCF_ReduceSideJoin_Mapper.class);
        job.setReducerClass(VCF_ReduceSideJoin_Reducer.class);
        job.setPartitionerClass(NaturalKeyPartitioner.class);
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);

        job.setInputFormatClass(VCFInputFormat.class);
        job.setMapOutputKeyClass(ChromPositionOrderKey.class);
        job.setMapOutputValueClass(VariantContextWritable.class);

        job.setOutputKeyClass(ChromPositionOrderKey.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true)) {
            System.err.println("sort :: Job failed.");
        }
    }

}