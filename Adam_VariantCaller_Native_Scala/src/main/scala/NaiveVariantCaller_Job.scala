package main.scala

import genepi.hadoop.HdfsUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.AlignmentRecord

import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of InnsbruckØ
  * Created 05.02.16.
  */
object NaiveVariantCaller_Job {
  private val parquetFileEnding = ".parquet.adam"

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("usage: spark-submit Adam_VariantCaller_Native_Scala-1.0-SNAPSHOT.jar <bam input directory> <parquet file name> <output dir>")
      return;
    }

    val inputPath = args(0)
    val parquetFileFolder = args(1)
    val outputPath = args(2)

    val conf = new SparkConf()
    conf.setAppName("Adam_VariantCaller_Native_Scala")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator","org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
    conf.registerKryoClasses(Array(classOf[BaseSequenceContent], classOf[NaiveVariantCallerKey]))
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.inputdir", inputPath)
    val ac = new ADAMContext(sc)

    //convert BamFiles to ADAM
    var samFileRDD: RDD[AlignmentRecord] = null

    if (inputPath.toLowerCase().endsWith(".bam") || inputPath.toLowerCase().endsWith(".sam")) {
      val parentFolderPath = inputPath.substring(0, inputPath.lastIndexOf("/"))
      val hdfsFilePaths: List[String] = HdfsUtil.getFiles(parentFolderPath)
      samFileRDD = ac.loadAlignments(hdfsFilePaths.get(0))
    } else {
      val filePaths = new ListBuffer[Path]
      val hdfsFilePaths: List[String] = HdfsUtil.getFiles(inputPath)
      hdfsFilePaths.foreach( filePath => filePaths += new Path(filePath) )
      if (filePaths.size == 0) {
        throw new IllegalArgumentException("input folder is empty")
      }
      samFileRDD = ac.loadAlignmentsFromPaths(filePaths)
    }

    //store as parquet files
    val parquetFilePath = parquetFileFolder + "/tmp" + parquetFileEnding
    samFileRDD.adamParquetSave(parquetFilePath)

    //map with index
    val parquetFileRDD: RDD[AlignmentRecord] = ac.loadAlignments(parquetFilePath)
    val myRDD: RDD[Pair[Int,AlignmentRecord]] = parquetFileRDD.mapPartitionsWithIndex{ (index, iterator) =>
      iterator.map{ record => (index, record)}
    }

    //filter step
    val filteredParquetFileRDD: RDD[Pair[Int,AlignmentRecord]] = myRDD.filter( record => NaiveVariantCaller_Filter.readFullfillsRequirements(record._2) )

    //mapping step
    val baseCount: RDD[Pair[NaiveVariantCallerKey,Char]] = filteredParquetFileRDD.flatMap( record => NaiveVariantCaller_Mapper.flatMap(record._1, record._2) )

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
    sortedRes.map(record => record._1 + "," + record._2).saveAsTextFile(outputPath)
  }

}