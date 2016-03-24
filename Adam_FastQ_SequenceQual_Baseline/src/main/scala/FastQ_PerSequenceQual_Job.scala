package main.scala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.io.SingleFastqInputFormat
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.misc.HadoopUtil

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 05.02.16.
  */
object FastQ_PerSequenceQual_Job {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("usage: spark-submit Adam_FastQ_SequenceQual_Baseline-1.0-SNAPSHOT.jar <fastq input directory> <output dir>")
      return;
    }

    val input = args(0)
    val output = args(1)

    val conf = new SparkConf()
    conf.setAppName("Adam_FastQ_SequenceQual_Baseline")
    val sc = new SparkContext(conf)

    val configuration = new Configuration()
    configuration.set("mapreduce.input.fileinputformat.inputdir", input)

    val job = HadoopUtil.newJob(sc)
    val fastQInput = sc.newAPIHadoopFile(
      input,
      classOf[SingleFastqInputFormat],
      classOf[Void],
      classOf[Text],
      ContextUtil.getConfiguration(job)
    )

    val hadoopRdd = fastQInput.asInstanceOf[NewHadoopRDD[Void, Text]]
    val myRdd: RDD[Pair[String, Text]] = hadoopRdd.mapPartitionsWithInputSplit { (inputSplit, iterator) ⇒
      val file = inputSplit.asInstanceOf[FileSplit]
      iterator.map(record => (file.getPath.getName, record._2))
    }

    val adamRDD: RDD[Pair[String, AlignmentRecord]] = myRdd.map(record => FastQ_PerSequenceQual_Mapper.mapSampleIdentifierWithConvertedInputObject(record))

    //mapping step
    val qualityScores: RDD[Pair[Pair[String, Int], Int]] = adamRDD.map(record => FastQ_PerSequenceQual_Mapper.map(record._1, record._2))

    //reduce step
    val res = qualityScores.reduceByKey(
      (a: Int, b: Int) => FastQ_PerSequenceQual_Reducer.combine(a, b)
    )

    //format and save output to file
    res.map(record => record._1._1 + "," + record._1._2 + "," + record._2).saveAsTextFile(output)
  }

}