package main.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.AlignmentRecord

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 07.04.16.
  */
object Adam_Transformer {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("usage: <input file> <output dir>")
      return;
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf()
    conf.setAppName("Adam_FastQ_BaseQual_Baseline")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.inputdir", inputPath)
    val ac = new ADAMContext(sc)

    val fastQFileRDD: RDD[AlignmentRecord] = ac.loadAlignments(inputPath)
    fastQFileRDD.adamParquetSave(outputPath)
  }

}