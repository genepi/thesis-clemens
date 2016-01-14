package main.scala.PerBaseSequenceContent

import main.scala.utils.BaseSequenceContent
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
object FastQ_PerBaseSequenceContent_Job {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("usage: spark-submit Spark_FastQ_Scala-1.0-SNAPSHOT.jar <fastQ input file> <output dir>")
      return;
    }

    val input = args(0)
    val output = args(1)

    val conf = new SparkConf().setAppName("Spark_FastQ_PerBaseSequenceContent_Scala")
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
    val baseCount: RDD[Pair[Int,Char]] = myRdd.flatMap( record => FastQ_PerBaseSequenceContent_Mapper.flatMap(record) )

    //reduce step
    val baseSequenceContent: RDD[Pair[Int,BaseSequenceContent]] = baseCount.combineByKey(
      (base: Char) => FastQ_PerBaseSequenceContent_Reducer.createBaseSeqContent(base),
      (bsc: BaseSequenceContent, base: Char) => FastQ_PerBaseSequenceContent_Reducer.countAndCalculateBasePercentage(bsc, base),
      (a: BaseSequenceContent, b: BaseSequenceContent) => FastQ_PerBaseSequenceContent_Reducer.combine(a, b)
    )

//    baseSequenceContent.persist()
//    baseSequenceContent.foreach( record => record._2.calculatePercentageOfBaseOccurrences() )

    //sort result
    val sortedRes = baseSequenceContent.sortBy( record => record._1 )

    //format and save output to file
    sortedRes.map( record => record._1 + "\t" + record._2).saveAsTextFile(output)
  }

}