package main.scala

import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.projections.{AlignmentRecordField, Projection}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.formats.avro.AlignmentRecord

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 21.04.2016
  */
object FastQ_PerBaseQual_Job {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("usage: spark-submit Adam_FastQ_BaseQual_SingleFile_Baseline-1.0.jar <fastq input directory> <tmp parquet output directory> <output dir>")
      return;
    }

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
    System.setProperty("spark.kryoserializer.buffer.mb", "4")
    System.setProperty("spark.kryo.referenceTracking", "true")

    val conf = new SparkConf()
    conf.setAppName("Adam_FastQ_BaseQual_SingleFile_Baseline")
    val sc = new SparkContext(conf)

    val input = args(0)
    val tmpFolder = args(1)
    val output = args(2)

    //conversion from fast-file into adam parquet format
    val startTime = System.nanoTime()

    val adamAlignments: AlignmentRecordRDD = sc.loadAlignments(input)
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
    val inputRdd: RDD[AlignmentRecord] = sc.loadAlignments(
      tmpFolder,
      projection = Some(
        Projection(
          AlignmentRecordField.qual
        )
      )
    ).map(record => record)

    //mapping step
    val qualityScores: RDD[Pair[Int, Int]] = inputRdd.flatMap( record => FastQ_PerBaseQual_Mapper.flatMap(record) )

    //reduce step
    val countedQualityScores: RDD[Pair[Int, AvgCount]] = qualityScores.combineByKey(
      (qualVal: Int) => FastQ_PerBaseQual_Reducer.createAverageCount(qualVal),
      (a: AvgCount, qualVal: Int) => FastQ_PerBaseQual_Reducer.addAndCount(a, qualVal),
      (a: AvgCount, b: AvgCount) => FastQ_PerBaseQual_Reducer.combine(a, b)
    )

    val res: RDD[Pair[Int, Double]] = countedQualityScores.map( record => FastQ_PerBaseQual_Mapper.map(record) );

    //format and save output to file
    res.map( record => record._1 + "," + record._2 ).saveAsTextFile(output)
  }

}