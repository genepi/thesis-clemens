package main.scala

import main.scala.utils.AvgCount
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.seqdoop.hadoop_bam.{FastqInputFormat, SequencedFragment}

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 21.04.2016
  */
object FastQ_PerBaseQual_Job {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("usage: spark-submit Spark_FastQ_BaseQual_Baseline-1.0-SNAPSHOT.jar <fastQ input file> <output dir>")
      return;
    }

    val input = args(0)
    val output = args(1)

    val conf = new SparkConf()
    conf.setAppName("Spark_FastQ_PerBaseQual_Baseline")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[AvgCount]))
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
    val qualityScores: RDD[Pair[Pair[String, Int], Int]] = myRdd.flatMap( record => FastQ_PerBaseQual_Mapper.flatMap(record._1, record._2) )

    //reduce step
    val countedQualityScores: RDD[Pair[Pair[String, Int], AvgCount]] = qualityScores.combineByKey(
      (qualVal: Int) => FastQ_PerBaseQual_Reducer.createAverageCount(qualVal),
      (a: AvgCount, qualVal: Int) => FastQ_PerBaseQual_Reducer.addAndCount(a, qualVal),
      (a: AvgCount, b: AvgCount) => FastQ_PerBaseQual_Reducer.combine(a, b)
    )

    val res: RDD[Pair[Pair[String, Int], Double]] = countedQualityScores.map( record => FastQ_PerBaseQual_Mapper.map(record) );

    res.map( record => record._1._1 + "," + record._1._2 + "," + record._2 ).saveAsTextFile(output)
  }

}