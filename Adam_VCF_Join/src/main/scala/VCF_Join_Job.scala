package main.scala

import org.apache.hadoop.io.LongWritable
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.misc.HadoopUtil
import org.seqdoop.hadoop_bam.{VCFInputFormat, VariantContextWritable}

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 13.02.16.
  */
object VCF_Join_Job {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("usage: spark-submit Adam_FastQ_BaseQual_Baseline-1.0-SNAPSHOT.jar <sample input file> <reference input file> <output dir>")
      return;
    }

    val sample = args(0)
    val reference = args(1)
    val output = args(2)

    val conf = new SparkConf()
    conf.setAppName("Adam_VCF_Join")
    val sc = new SparkContext(conf)


    val ac = new ADAMContext(sc)
    val abc: AlignmentRecordRDD = ac.loadAlignments("filename");

//    abc.recordGroups.


    val job = HadoopUtil.newJob(sc)
    val vcc = new VariantContextConverter()

    //load vcf sample input data into a RDD
    val vcfSampleInput = sc.newAPIHadoopFile(
      sample,
      classOf[VCFInputFormat],
      classOf[LongWritable],
      classOf[VariantContextWritable],
      ContextUtil.getConfiguration(job)
    )

    //convert data into ADAMs VariantContext format
    val sampleRDD: RDD[VariantContext] = vcfSampleInput.flatMap(p => vcc.convert(p._2.get))

    //define the join key on the sample RDD
    val sampleJoinRDD: RDD[Pair[Pair[Int, Int], VariantContext]] = sampleRDD.keyBy( record => VCF_Join_Mapper.mapKeyBy(record.variant) )

    //load vcf reference input data into a RDD
    val vcfReferenceInput = sc.newAPIHadoopFile(
      reference,
      classOf[VCFInputFormat],
      classOf[LongWritable],
      classOf[VariantContextWritable],
      ContextUtil.getConfiguration(job)
    )

    //convert data into ADAMs VariantContext format
    val referenceRDD: RDD[VariantContext] = vcfReferenceInput.flatMap(p => vcc.convert(p._2.get()))

    //define the join key on the sample RDD
    val referenceJoinRDD: RDD[Pair[Pair[Int, Int], VariantContext]] = referenceRDD.keyBy( record => VCF_Join_Mapper.mapKeyBy(record.variant) )

    // join the two RDDs according to their key (composed of chromosome & position)
    val joinedRDD: RDD[Pair[Pair[Int, Int], Pair[VariantContext, Option[VariantContext]]]] = sampleJoinRDD.leftOuterJoin(referenceJoinRDD)

    joinedRDD.map(record => record._1).saveAsTextFile(output)

    // TODO implement result generation
//    val res = joinedRDD.sortBy( record => record._1 ).map( record => VCF_Join_Mapper.constructResult(record._1, record._2) )
//    res.saveAsTextFile(output)
  }

}
