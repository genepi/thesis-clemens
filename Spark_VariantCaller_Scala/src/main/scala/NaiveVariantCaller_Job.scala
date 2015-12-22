package main.scala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.seqdoop.hadoop_bam.{BAMInputFormat, FileVirtualSplit, SAMRecordWritable}

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 26.11.15.
  */
object NaiveVariantCaller_Job {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("usage: /Users/Clemens/thesis/binaries/spark-1.4.1-bin-hadoop2.6/bin/spark-submit --master local[2] /Users/Clemens/thesis/Spark_VariantCaller_Scala/target/Spark_VariantCaller_Scala-1.0-SNAPSHOT.jar <bam input file> <output dir>")
      return;
    }

    val input = args(0)
    val output = args(1)

    val conf = new SparkConf().setAppName("Spark_VariantCaller_Scala")
    val sc = new SparkContext(conf)

    val configuration = new Configuration()
    configuration.set("mapreduce.input.fileinputformat.inputdir", input)

    val bamFileRDD = sc.newAPIHadoopRDD( //reads a bam file from HDFS
      configuration,
      classOf[BAMInputFormat],
      classOf[LongWritable],
      classOf[SAMRecordWritable]
    )

    //retrieve the filename
    val hadoopRdd = bamFileRDD.asInstanceOf[NewHadoopRDD[LongWritable,SAMRecordWritable]]
    val myRdd: RDD[Pair[String,SAMRecordWritable]] = hadoopRdd.mapPartitionsWithInputSplit { (inputSplit, iterator) ⇒
      val file = inputSplit.asInstanceOf[FileVirtualSplit]
      iterator.map { record ⇒ (file.getPath.getName, record._2) }
    }

    //mapping step
    val baseCount: RDD[Pair[NaiveVariantCallerKey,Char]] = myRdd.flatMap( a => NaiveVariantCaller_Mapper.flatMap(a._1, a._2) )

    //reduce step
    val baseSequenceContent: RDD[Pair[NaiveVariantCallerKey,BaseSequenceContent]] = baseCount.combineByKey(
      (base: Char) => (NaiveVariantCaller_Reducer.createBaseSeqContent(base)),
      (bsc: BaseSequenceContent, base: Char) => (NaiveVariantCaller_Reducer.countAndCalculateBasePercentage(bsc,base)),
      (bsc1: BaseSequenceContent, bsc2: BaseSequenceContent) => (NaiveVariantCaller_Reducer.combine(bsc1,bsc2)))

    //filter step
    val res: RDD[Pair[NaiveVariantCallerKey,BaseSequenceContent]] = baseSequenceContent.filter(
      record => NaiveVariantCaller_Filter.filterLowClarityAndReferenceMatchingBases(record)
    )

    //sort result
    val sortedRes = res.sortBy( record => (record._1.getSampleIdentifier(), record._1.getPosition()) )

    //format and save output to file
    sortedRes.map(record => record._1 + "," + record._2).saveAsTextFile(output)
  }

}