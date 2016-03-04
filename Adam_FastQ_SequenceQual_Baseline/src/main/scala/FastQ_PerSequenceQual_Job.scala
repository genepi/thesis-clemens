package main.scala

import genepi.hadoop.HdfsUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.projections.{AlignmentRecordField, Projection}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.AlignmentRecord

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

    if (inputPath.toLowerCase().endsWith(".fastq") || inputPath.toLowerCase().endsWith(".fq")) {
      val parentFolderPath = inputPath.substring(0, inputPath.lastIndexOf("/"))
      val hdfsFilePaths: List[String] = HdfsUtil.getFiles(parentFolderPath)
      this.processJob(ac, hdfsFilePaths.get(0), parquetFileFolder, outputPath)
    } else {
      val hdfsFilePaths: List[String] = HdfsUtil.getFiles(inputPath)
      if (hdfsFilePaths.size == 0) {
        throw new IllegalArgumentException("input folder is empty")
      }
      hdfsFilePaths.foreach( path => this.processJob(ac, path, parquetFileFolder, outputPath) )
    }
  }

  private def processJob(ac: ADAMContext, filePath: String, parquetFileFolder: String, outputPath: String): Unit = {
    //convert FastQ files to ADAM
    val fastQFileRDD: RDD[AlignmentRecord] = ac.loadAlignments(filePath)
    val fileName = filePath.substring(filePath.lastIndexOf('/')+1, filePath.length())

    //store as parquet files
    val parquetFilePath = parquetFileFolder + "/" + fileName + parquetFileEnding
    fastQFileRDD.adamParquetSave(parquetFilePath)

    //load parquet file
    val parquetFileRDD: RDD[AlignmentRecord] = ac.loadAlignments(
      parquetFilePath,
      projection = Some(
        Projection(
          AlignmentRecordField.qual
        )
      )
    )

    //mapping step
    val qualityScores: RDD[Pair[Int, Int]] = parquetFileRDD.map( record => FastQ_PerSequenceQual_Mapper.map( record ) )

    //reduce step
    val res = qualityScores.reduceByKey(
      (a: Int, b: Int) => FastQ_PerSequenceQual_Reducer.combine(a, b)
    )

    //format and save output to file
    res.sortBy(record => (record._1)).map( record => fileName + "," + record._1 + "," + record._2 ).saveAsTextFile(outputPath + "/" + fileName.substring(0, fileName.lastIndexOf('.')))
  }

}