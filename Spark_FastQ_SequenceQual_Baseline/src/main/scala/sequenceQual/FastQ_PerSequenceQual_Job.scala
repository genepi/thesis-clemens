package main.scala.sequenceQual

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.seqdoop.hadoop_bam.{SequencedFragment, FastqInputFormat}

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

    val conf = new SparkConf().setAppName("Spark_FastQ_PerSequenceQual_Baseline")
    val sc = new SparkContext(conf)

    val configuration = new Configuration()
    configuration.set("mapreduce.input.fileinputformat.inputdir", input)

    val fastQFileRDD = sc.newAPIHadoopRDD( //reads a fastQ file from HDFS
      configuration,
      classOf[FastqInputFormat],
      classOf[Text],
      classOf[SequencedFragment]
    ).values

    //mapping step
    val qualityScores: RDD[Pair[Int,Int]] = fastQFileRDD.map( record => FastQ_PerSequenceQual_Mapper.map( record ) )

    //reduce step
    val res = qualityScores.reduceByKey(
      (a: Int, b: Int) => FastQ_PerSequenceQual_Reducer.combine(a, b)
    )

    //sort result
    val sortedRes = res.sortBy( record => record._1 )

    //format and save output to file
    sortedRes.map( record => record._1 + "," + record._2 ).saveAsTextFile(output)
  }

}