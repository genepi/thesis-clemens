package main.scala

import genepi.hadoop.HdfsUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.AlignmentRecord


/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 13.02.16.
  */
object VCF_Join_Job {
  private val parquetFileEnding = ".parquet.adam"

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("usage: spark-submit Adam_FastQ_BaseQual_Baseline-1.0-SNAPSHOT.jar <vcf small input file> <vcf large input file> <parquet file name> <output dir>")
      return;
    }

    val vcfSmallInput = args(0)
    val vcfLargeInput = args(1)
    val parquetFileFolder = args(2)
    val outputPath = args(3)

    val conf = new SparkConf()
    conf.setAppName("Adam_VCF_Join")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.inputdir", vcfSmallInput)
    val ac = new ADAMContext(sc)

    //convert FastQ files to ADAM
    val parentFolderPath = vcfSmallInput.substring(0, vcfSmallInput.lastIndexOf("/"))
    val hdfsFilePaths: List[String] = HdfsUtil.getFiles(parentFolderPath)
    var smallVcfFileRDD: RDD[AlignmentRecord] = ac.loadAlignments(hdfsFilePaths.get(0))

    //store as parquet files
    val parquetFilePath = parquetFileFolder + "/tmp" + parquetFileEnding
    smallVcfFileRDD.adamParquetSave(parquetFilePath)

    //load parquet file
    val parquetFileRDD: RDD[AlignmentRecord] = ac.loadAlignments(parquetFilePath)

  }

}
