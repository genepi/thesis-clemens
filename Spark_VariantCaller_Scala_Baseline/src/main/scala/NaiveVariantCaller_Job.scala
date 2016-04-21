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
  * Created 21.04.2016
  */
object NaiveVariantCaller_Job {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("usage: spark-submit Spark_VariantCaller_Scala_Baseline-1.0.jar <bam input file> <output dir>")
      return;
    }

    val input = args(0)
    val output = args(1)

    val conf = new SparkConf()
    conf.setAppName("Spark_VariantCaller_Scala_Baseline")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[BaseSequenceContent], classOf[NaiveVariantCallerKey]))
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
    val myRdd: RDD[Pair[String, SAMRecord]] = hadoopRdd.values.map(record => record.get()).keyBy(record => record.getHeader.getReadGroups.get(0).getSample)

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

    //format and save output to file
    res.map(record => record._1 + "," + record._2).saveAsTextFile(output)
  }

}