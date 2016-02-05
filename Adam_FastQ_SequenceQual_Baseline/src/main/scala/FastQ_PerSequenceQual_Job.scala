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
  * Organization: DBIS - University of Innsbruck
  * Created 05.02.16.
  */
object FastQ_PerSequenceQual_Job {
  private val parquetFileEnding = ".parquet.adam"

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("usage: spark-submit Adam_FastQ_SequenceQual_Baseline-1.0-SNAPSHOT.jar <fastq input directory> <parquet file name> <output dir>")
      return;
    }

    val inputPath = args(0)
    val parquetFileFolder = args(1)
    val outputPath = args(2)

    val conf = new SparkConf()
    conf.setAppName("Adam_FastQ_SequenceQual_Baseline")
    val sc = new SparkContext(conf)

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
    val qualityScores: RDD[Pair[Int,Int]] = parquetFileRDD.map( record => FastQ_PerSequenceQual_Mapper.map( record ) )

    //reduce step
    val res = qualityScores.reduceByKey(
      (a: Int, b: Int) => FastQ_PerSequenceQual_Reducer.combine(a, b)
    )

    //sort result
    val sortedRes = res.sortBy( record => record._1 )

    //format and save output to file
    sortedRes.map( record => record._1 + "," + record._2 ).saveAsTextFile(outputPath)
  }

}