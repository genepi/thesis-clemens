package main.scala

import genepi.hadoop.HdfsUtil
import main.utils.AvgCount
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
object FastQ_PerBaseQual_Job {
  private val parquetFileEnding = ".parquet.adam"

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("usage: spark-submit Adam_FastQ_BaseQual_Baseline-1.0-SNAPSHOT.jar <fastq input directory> <parquet file name> <output dir>")
      return;
    }

    val inputPath = args(0)
    val parquetFileFolder = args(1)
    val outputPath = args(2)

    val sc = new SparkContext(new SparkConf()
      .setAppName("Adam_FastQ_BaseQual_Baseline")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator","org.bdgenomics.adam.serialization.ADAMKryoRegistrator"))
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.inputdir", inputPath)
    val ac = new ADAMContext(sc)

    //convert FastQ files to ADAM
    var fastQFileRDD: RDD[AlignmentRecord] = null

    if (inputPath.toLowerCase().endsWith(".fastq") || inputPath.toLowerCase().endsWith(".fq")) {
      val parentFolderPath = inputPath.substring(0, inputPath.lastIndexOf("/"))
      val hdfsFilePaths: List[String] = HdfsUtil.getFiles(parentFolderPath)
      fastQFileRDD = ac.loadAlignments(hdfsFilePaths.get(0))
    } else {
      val filePaths = new ListBuffer[Path]
      val hdfsFilePaths: List[String] = HdfsUtil.getFiles(inputPath)
      hdfsFilePaths.foreach( filePath => filePaths += new Path(filePath) )
      if (filePaths.size == 0) {
        throw new IllegalArgumentException("input folder is empty")
      }
      fastQFileRDD = ac.loadAlignmentsFromPaths(filePaths)
    }

    //store as parquet files
    val parquetFilePath = parquetFileFolder + "/tmp" + parquetFileEnding
    fastQFileRDD.adamParquetSave(parquetFilePath)

    //load parquet file
    val parquetFileRDD: RDD[AlignmentRecord] = ac.loadAlignments(parquetFilePath)

    //mapping step
    val qualityScores: RDD[Pair[Int, Int]] = parquetFileRDD.flatMap( record => FastQ_PerBaseQual_Mapper.flatMap(record) )

    //reduce step
    val countedQualityScores: RDD[Pair[Int, AvgCount]] = qualityScores.combineByKey(
      (qualVal: Int) => FastQ_PerBaseQual_Reducer.createAverageCount(qualVal),
      (a: AvgCount, qualVal: Int) => FastQ_PerBaseQual_Reducer.addAndCount(a, qualVal),
      (a: AvgCount, b: AvgCount) => FastQ_PerBaseQual_Reducer.combine(a, b)
    )

    val res: RDD[Pair[Int, Double]] = countedQualityScores.map( record => FastQ_PerBaseQual_Mapper.map(record) );

    res.sortBy( record => record._1 ).map( record => record._1 + "," + record._2 ).saveAsTextFile(outputPath)
  }

}