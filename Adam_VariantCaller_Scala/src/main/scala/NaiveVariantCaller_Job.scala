package main.scala

import genepi.hadoop.HdfsUtil
import htsjdk.samtools.SAMFileHeader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.models.SAMFileHeaderWritable
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.AlignmentRecord
import org.seqdoop.hadoop_bam.util.SAMHeaderReader

import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of InnsbruckØ
  * Created 27.11.15.
  */
object NaiveVariantCaller_Job {
  private val parquetFileEnding = ".parquet.adam"

  def main(args: Array[String]): Unit = {

    println("starting the ADAM job")

    println("\n\narguments following:")
    println(args)

    if (args.length != 3) {
      println("usage: /Users/Clemens/thesis/binaries/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --master local[2] /Users/Clemens/thesis/Adam_VariantCaller_Scala/target/Adam_VariantCaller_Scala-1.0-SNAPSHOT.jar <bam input directory> <parquet file name> <output dir>")
      return;
    }

    val inputPath = args(0)
    val parquetFileFolder = args(1)
    val outputPath = args(2)
    var samInputFilePath = ""

    val sc = new SparkContext(new SparkConf()
      .setAppName("Adam_VariantCaller_Scala")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator","org.bdgenomics.adam.serialization.ADAMKryoRegistrator"))
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.inputdir", inputPath)
    val ac = new ADAMContext(sc)

    //convert BamFiles to ADAM
    var samFileRDD: RDD[AlignmentRecord] = null

    if (inputPath.toLowerCase().endsWith(".bam") || inputPath.toLowerCase().endsWith(".sam")) {
      val parentFolderPath = inputPath.substring(0, inputPath.lastIndexOf("/"))
      val hdfsFilePaths: List[String] = HdfsUtil.getFiles(parentFolderPath)
      samFileRDD = ac.loadAlignments(hdfsFilePaths.get(0))
      samInputFilePath = hdfsFilePaths.get(0)
    } else {
      val filePaths = new ListBuffer[Path]
      val hdfsFilePaths: List[String] = HdfsUtil.getFiles(inputPath)
      hdfsFilePaths.foreach( filePath => filePaths += new Path(filePath) )
      if (filePaths.size == 0) {
        throw new IllegalArgumentException("input folder is empty")
      }
      samFileRDD = ac.loadAlignmentsFromPaths(filePaths)
      samInputFilePath = hdfsFilePaths.get(0)
    }

    //store as parquet files
    val parquetFilePath = parquetFileFolder + "/tmp" + parquetFileEnding
    samFileRDD.adamParquetSave(parquetFilePath)

    //map with index
    val parquetFileRDD: RDD[AlignmentRecord] = ac.loadAlignments(parquetFilePath)
    val myRDD: RDD[Pair[Int,AlignmentRecord]] = parquetFileRDD.mapPartitionsWithIndex{ (index, iterator) =>
      iterator.map{ record => (index,record)}
    }

    //needed to convert AlignmentRecords to SAMRecords
    val recordConverter = new AlignmentRecordConverter()
    val sfh: SAMFileHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(samInputFilePath), new Configuration())
    val sfhWritable = new SAMFileHeaderWritable(sfh)

    //mapping step
    val baseCount: RDD[Pair[NaiveVariantCallerKey,Char]] = myRDD.flatMap( a => NaiveVariantCaller_Mapper.flatMap(a._1, a._2, recordConverter, sfhWritable))

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