package main.scala

import htsjdk.variant.variantcontext.VariantContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.seqdoop.hadoop_bam.{VCFInputFormat, VariantContextWritable}

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 13.02.16.
  */
object VCF_Join_Job {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("usage: spark-submit Spark_VCF_Join-1.0-SNAPSHOT.jar <vcf small input file> <vcf reference input file> <output dir>")
      return;
    }

    val vcfSmallInput = args(0)
    val vcfLargeInput = args(1)
    val output = args(2)

    val conf = new SparkConf()
    conf.setAppName("Spark_VCF_Join")
    val sc = new SparkContext(conf)


    // load the small VCF dataset into an RDD
    val configurationVcfInputSmall = new Configuration()
    configurationVcfInputSmall.set("mapreduce.input.fileinputformat.inputdir", vcfSmallInput)

    val vcfSmallFileRDD: RDD[VariantContextWritable] = sc.newAPIHadoopRDD( //reads a VCF file from HDFS
      configurationVcfInputSmall,
      classOf[VCFInputFormat],
      classOf[LongWritable],
      classOf[VariantContextWritable]
    ).values

    // define the join key on the small RDD
    val vcfSmallFileRDDMapped: RDD[VariantContext] = vcfSmallFileRDD.map( record => record.get() )
    val smallJoinRDDRelation: RDD[Pair[Pair[Int, Int], VariantContext]] = vcfSmallFileRDDMapped.keyBy( record => VCF_Join_Mapper.mapKeyBy(record) )

    // load the large VCF dataset into an RDD
    val configurationVCFInputLarge = new Configuration()
    configurationVCFInputLarge.set("mapreduce.input.fileinputformat.inputdir", vcfLargeInput)

    val vcfLargeFileRDD = sc.newAPIHadoopRDD( //reads a VCF file from HDFS
      configurationVCFInputLarge,
      classOf[VCFInputFormat],
      classOf[LongWritable],
      classOf[VariantContextWritable]
    ).values

    // define the join key on the large RDD
    val vcfLargeFileRDDMapped: RDD[VariantContext] = vcfLargeFileRDD.map( record => record.get() )
    val largeJoinRDDRelation: RDD[Pair[Pair[Int, Int], VariantContext]] = vcfLargeFileRDDMapped.keyBy( record => VCF_Join_Mapper.mapKeyBy(record) )

    // join the two RDDs according to their key (composed of chromosome & position)
    val joinedRDD: RDD[Pair[Pair[Int, Int], Pair[VariantContext, Option[VariantContext]]]] = smallJoinRDDRelation.leftOuterJoin(largeJoinRDDRelation)

    val res = joinedRDD.sortBy( record => record._1 ).map( record => VCF_Join_Mapper.constructResult(record._1, record._2) )
    res.saveAsTextFile(output)
  }

}