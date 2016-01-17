package main.scala.PerBaseQual

import main.scala.utils.AvgCount
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.seqdoop.hadoop_bam.{FastqInputFormat, SequencedFragment}

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 02.01.16.
  */
object FastQ_PerBaseQual_Job {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("usage: spark-submit Spark_FastQ_Scala-1.0-SNAPSHOT.jar <fastQ input file> <output dir>")
      return;
    }

    val input = args(0)
    val output = args(1)

    val conf = new SparkConf().setAppName("Spark_FastQ_PerBaseQual_Scala")

    val sc = new SparkContext(conf)

    val configuration = new Configuration()
    configuration.set("mapreduce.input.fileinputformat.inputdir", input)

    val fastQFileRDD = sc.newAPIHadoopRDD( //reads a fastQ file from HDFS
      configuration,
      classOf[FastqInputFormat],
      classOf[Text],
      classOf[SequencedFragment]
    )

    val myRdd = fastQFileRDD.values

    //mapping step
    val qualityScores = myRdd.flatMap( record => FastQ_PerBaseQual_Mapper.flatMap(record) )

    //reduce step
    val res: RDD[Pair[Int,AvgCount]] = qualityScores.combineByKey(
      (value: Double) => FastQ_PerBaseQual_Reducer.createAverageCount(value),
      (a: AvgCount, x: Double) => FastQ_PerBaseQual_Reducer.addAndCount(a, x),
      (a: AvgCount, b: AvgCount) => FastQ_PerBaseQual_Reducer.combine(a, b)
    )

    res.sortBy( record => record._1 ).map( record => record._1 + "\t" + record._2 ).saveAsTextFile(output)
  }

}