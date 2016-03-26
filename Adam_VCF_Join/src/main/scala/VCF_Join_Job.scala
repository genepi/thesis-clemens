package main.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.AlignmentRecord


/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 13.02.16.
  */
object VCF_Join_Job {
  private val parquetFileEnding = ".parquet.adam"

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

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.inputdir", sample)
    val ac = new ADAMContext(sc)

    //load vcf sample input data into a RDD
    //and define the join key on the sample RDD
    var sampleRDD: RDD[AlignmentRecord] = ac.loadAlignments(sample)
    val sampleJoinRDD: RDD[Pair[Pair[Int, Int], AlignmentRecord]] = sampleRDD.keyBy( record => VCF_Join_Mapper.mapKeyBy(record) )

    //load vcf reference input data into a RDD
    //and define the join key on the reference RDD
    var referenceRDD: RDD[AlignmentRecord] = ac.loadAlignments(reference)
    val referenceJoinRDD: RDD[Pair[Pair[Int, Int], AlignmentRecord]] = referenceRDD.keyBy( record => VCF_Join_Mapper.mapKeyBy(record) )

    // join the two RDDs according to their key (composed of chromosome & position)
    val joinedRDD: RDD[Pair[Pair[Int, Int], Pair[AlignmentRecord, Option[AlignmentRecord]]]] = sampleJoinRDD.leftOuterJoin(referenceJoinRDD)

    //TODO implement result generation
    val res = joinedRDD.sortBy( record => record._1 ).map( record => VCF_Join_Mapper.constructResult(record._1, record._2) )
    res.saveAsTextFile(output)
  }

}
