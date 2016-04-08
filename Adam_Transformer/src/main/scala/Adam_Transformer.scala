package main.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD

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
    conf.setAppName("Adam_Transformer")
    val sc = new SparkContext(conf)

    if (inputPath.toLowerCase().endsWith(".vcf")) {
      // https://github.com/bigdatagenomics/adam/blob/b29d20480001be20e0be55e828a147b5f222ce42/adam-cli/src/main/scala/org/bdgenomics/adam/cli/Vcf2ADAM.scala
      val adamVariants: RDD[VariantContext] = sc.loadVcf(inputPath, None)
//      adamVariants.map(v => v.variant.variant).adamParquetSave(outputPath)
      adamVariants.flatMap(p => p.genotypes).adamParquetSave(outputPath)

    } else if (inputPath.toLowerCase().endsWith(".fastq") || inputPath.toLowerCase().endsWith(".bam")) {
      val adamAlignments: AlignmentRecordRDD = sc.loadAlignments(inputPath)
      adamAlignments.map(record => record).adamParquetSave(outputPath)
    }
  }

}


//TODO remove old code
//    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.inputdir", inputPath)
//    val ac = new ADAMContext(sc)

//    //convert files to ADAM parquet format
//    var inputRDD: RDD[AlignmentRecord] = null
//
//    if (inputPath.toLowerCase().endsWith(".fastq") || inputPath.toLowerCase().endsWith(".bam") || inputPath.toLowerCase().endsWith(".vcf")) {
//      val parentFolderPath = inputPath.substring(0, inputPath.lastIndexOf("/"))
//      val hdfsFilePaths: List[String] = HdfsUtil.getFiles(parentFolderPath)
//      inputRDD = ac.loadAlignments(hdfsFilePaths.get(0))
//    } else {
//      val filePaths = new ListBuffer[Path]
//      val hdfsFilePaths: List[String] = HdfsUtil.getFiles(inputPath)
//      hdfsFilePaths.foreach( filePath => filePaths += new Path(filePath) )
//      if (filePaths.size == 0) {
//        throw new IllegalArgumentException("input folder is empty")
//      }
//      inputRDD = ac.loadAlignmentsFromPaths(filePaths)
//    }
//
//    //store as parquet files
//    inputRDD.adamParquetSave(outputPath)