package main.scala

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.seqdoop.hadoop_bam.{BAMInputFormat, FileVirtualSplit, SAMRecordWritable}

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 29.01.16.
  */
object NaiveVariantCaller_Job {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("usage: spark-submit Spark_VariantCaller_Scala-1.0-SNAPSHOT.jar <bam input file> <output dir>")
      return;
    }

    val input = args(0)
    val output = args(1)

    val conf = new SparkConf()
    conf.setAppName("Spark_VariantCaller_Scala")
    conf.registerKryoClasses(Array(classOf[BaseSequenceContent]))
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
    val myRdd: RDD[Pair[String, SAMRecord]] = hadoopRdd.mapPartitionsWithInputSplit { (inputSplit, iterator) ⇒
      val file = inputSplit.asInstanceOf[FileVirtualSplit]
      iterator.map { record ⇒ (file.getPath.getName, record._2.get()) }
    }

    val preFilter: RDD[Pair[String, SAMRecord]] = myRdd.filter( record => NaiveVariantCaller_Filter.readFullfillsRequirements(record._2) )
    val baseCount: RDD[Pair[Pair[String, Int], Char]] = preFilter.flatMap( record => NaiveVariantCaller_Mapper.flatMap(record._1, record._2) )

    //reduce step
    val baseSequenceContent: RDD[Pair[Pair[String, Int], BaseSequenceContent]] = baseCount.combineByKey(
      (base: Char) => (NaiveVariantCaller_Reducer.createBaseSeqContent(base)),
      (bsc: BaseSequenceContent, base: Char) => (NaiveVariantCaller_Reducer.countAndCalculateBasePercentage(bsc,base)),
      (bsc1: BaseSequenceContent, bsc2: BaseSequenceContent) => (NaiveVariantCaller_Reducer.combine(bsc1,bsc2)))

    //map most-dominant base
    val baseSequenceContentMapped: RDD[Pair[Pair[String, Int], Char]] = baseSequenceContent.mapValues( record => NaiveVariantCaller_Mapper.mapMostDominantBase(record) )

    //filter step
    val res: RDD[Pair[Pair[String, Int], Char]] = baseSequenceContentMapped.filter(
      record => NaiveVariantCaller_Filter.filterLowClarityAndReferenceMatchingBases(record._1._2, record._2)
    )

    //format and save output to file
    res.map(record => record._1._1 + "," + record._1._2 + "," + record._2).saveAsTextFile(output)
  }

}