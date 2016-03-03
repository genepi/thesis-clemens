package main.scala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.seqdoop.hadoop_bam.{FastqInputFormat, SequencedFragment}

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 04.02.16.
  */
object FastQ_PerSequenceQual_Job {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("usage: spark-submit Spark_FastQ_SequenceQual_Baseline-1.0-SNAPSHOT.jar <fastQ input file> <output dir>")
      return;
    }

    val input = args(0)
    val output = args(1)

    val conf = new SparkConf()
    conf.setAppName("Spark_FastQ_PerSequenceQual_Baseline")
    val sc = new SparkContext(conf)

    val configuration = new Configuration()
    configuration.set("mapreduce.input.fileinputformat.inputdir", input)

    val fastQFileRDD = sc.newAPIHadoopRDD( //reads a fastQ file from HDFS
      configuration,
      classOf[FastqInputFormat],
      classOf[Text],
      classOf[SequencedFragment]
    )

    //retrieve the filename
    val hadoopRdd = fastQFileRDD.asInstanceOf[NewHadoopRDD[Text, SequencedFragment]]
    val myRdd: RDD[Pair[String, SequencedFragment]] = hadoopRdd.mapPartitionsWithInputSplit { (inputSplit, iterator) ⇒
      val file = inputSplit.asInstanceOf[FileSplit]
      iterator.map { record ⇒ (file.getPath.getName, record._2) }
    }

    //mapping step
    val qualityScores: RDD[Pair[Pair[String, Int],Int]] = myRdd.map( record => FastQ_PerSequenceQual_Mapper.map( record._1, record._2 ) )

    //reduce step
    val res = qualityScores.reduceByKey(
      (a: Int, b: Int) => FastQ_PerSequenceQual_Reducer.combine(a, b)
    )

    //format and save output to file
    res.map( record => record._1._1 + "," + record._1._2  + "," + record._2 ).saveAsTextFile(output)
  }

}