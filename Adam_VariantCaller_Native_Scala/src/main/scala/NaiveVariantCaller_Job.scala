package main.scala

import genepi.hadoop.HdfsUtil
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.AlignmentRecord

import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of InnsbruckØ
  * Created 21.04.2016
  */
object NaiveVariantCaller_Job {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("usage: spark-submit Adam_VariantCaller_Native_Scala-1.0.jar <bam input directory> <tmp parquet output directory> <output dir>")
      return;
    }

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
    System.setProperty("spark.kryoserializer.buffer.mb", "4")
    System.setProperty("spark.kryo.referenceTracking", "true")

    val conf = new SparkConf()
    conf.setAppName("Adam_VariantCaller_Native_Scala")
    val sc = new SparkContext(conf)

    val input = args(0)
    val tmpFolder = args(1)
    val output = args(2)

    //conversion from bam-files containing folder into adam parquet format
    val startTime = System.nanoTime()

    val filePaths = new ListBuffer[Path]
    val hdfsFilePaths: List[String] = HdfsUtil.getFiles(input)
    hdfsFilePaths.foreach( filePath => filePaths += new Path(filePath) )
    if (filePaths.size == 0) {
      throw new IllegalArgumentException("input folder is empty")
    }

    val adamAlignments: AlignmentRecordRDD = sc.loadAlignmentsFromPaths(filePaths)
    val adamArgs = new ADAMSaveAnyArgs {
      override var sortFastqOutput: Boolean = false
      override var asSingleFile: Boolean = true
      override var outputPath: String = tmpFolder
      override var disableDictionaryEncoding: Boolean = false
      override var blockSize: Int = 128 * 1024 * 1024
      override var pageSize: Int = 1 * 1024 * 1024
      override var compressionCodec: CompressionCodecName = CompressionCodecName.GZIP
    }
    adamAlignments.map(r => r).saveAsParquet(adamArgs, adamAlignments.sequences, adamAlignments.recordGroups)
    val timeDiff = System.nanoTime() - startTime;
    System.out.println("time needed to perform conversion to adam: " + timeDiff);

    //processing of parquet file
    val inputRdd: AlignmentRecordRDD = sc.loadAlignments(tmpFolder)
    val adamRDD: RDD[Pair[String, AlignmentRecord]] = inputRdd.keyBy( record => record.getRecordGroupSample )

    val preFilter: RDD[Pair[String,AlignmentRecord]] = adamRDD.filter( record => NaiveVariantCaller_Filter.readFullfillsRequirements(record._2) )
    val richRecordMap: RDD[Pair[String,RichAlignmentRecord]] = preFilter.map( record => NaiveVariantCaller_Mapper.convertAlignmentRecordToRichAlignmentRecord(record._1, record._2) )
    val baseCount: RDD[Pair[Pair[String, Int], Char]] = richRecordMap.flatMap( record => NaiveVariantCaller_Mapper.flatMap(record._1, record._2) )

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