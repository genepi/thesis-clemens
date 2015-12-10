import java.io.{File, FileInputStream}

import htsjdk.samtools.{SAMFileHeader, SAMRecord}
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
  * Organization: DBIS - University of Innsbruck
  * Created 27.11.15.
  */
object NaiveVariantCaller_Job {
  private val parquetFileName = "parquet.adam"

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("usage: /Users/Clemens/thesis/binaries/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --master local[2] /Users/Clemens/thesis/Adam_VariantCaller_Scala/target/Adam_VariantCaller_Scala-1.0-SNAPSHOT.jar <bam input file> <output dir>")
      return;
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val sc = new SparkContext(new SparkConf()
      .setAppName("Adam_VariantCaller_Scala")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator","org.bdgenomics.adam.serialization.ADAMKryoRegistrator"))
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.inputdir", inputPath)
    val ac = new ADAMContext(sc)

    val inputFile: File = new File(inputPath)
    var samFileRDD: RDD[AlignmentRecord] = null
    var parquetFilePath = new String

    //convert BamFiles to ADAM
//    if (inputFile.isDirectory) {
//      val inputPaths = getRecursiveListOfFilePaths(inputFile);
//      parquetFilePath = new File(inputFile, parquetFileName).getAbsolutePath
//      samFileRDD = ac.loadAlignmentsFromPaths(inputPaths)
//    } else {
    val samInputFile: File = new File(inputPath);
    parquetFilePath = new File(samInputFile.getParentFile, parquetFileName).getAbsolutePath
    samFileRDD = ac.loadAlignments(inputPath)
//    }

    //store as parquet files
    samFileRDD.adamParquetSave(parquetFilePath)

    //load parquet files and convert AlignmentRecords to SAMRecords
    val parquetFileRDD: RDD[AlignmentRecord] = ac.loadAlignments(parquetFilePath)
    val recordConverter = new AlignmentRecordConverter()
    val sfh: SAMFileHeader = SAMHeaderReader.readSAMHeaderFrom(new FileInputStream(samInputFile), new Configuration())
    val sfhWritable = new SAMFileHeaderWritable(sfh)

    // TODO Achtung auf die Performance!!! => ist vielleicht nicht die schnellste Variante...
    val temp: Array[AlignmentRecord] = parquetFileRDD.collect()
    val samRecords: ListBuffer[SAMRecord] = new ListBuffer[SAMRecord]

    for (i <- 0 to temp.length-1) {
      if (NaiveVariantCaller_Filter.mappingQualitySufficient(temp(i).getMapq())) { //filter low mapping qual records
        val sr: SAMRecord = convertToSAMRecord(temp(i), recordConverter, sfhWritable)
        if (sr != null) {
          samRecords += sr
        }
      }
    }

    val samRecordsRDD: RDD[SAMRecord] = sc.parallelize(samRecords)

    //mapping step
    val baseCount: RDD[Pair[Int,Char]] = samRecordsRDD.flatMap( record => NaiveVariantCaller_Mapper.flatMap(record) )

    //reduce step
    val baseSequenceContent: RDD[Pair[Int,BaseSequenceContent]] = baseCount.combineByKey(
      (base: Char) => (NaiveVariantCaller_Reducer.createBaseSeqContent(base)),
      (bsc: BaseSequenceContent, base: Char) => (NaiveVariantCaller_Reducer.countAndCalculateBasePercentage(bsc,base)),
      (bsc1: BaseSequenceContent, bsc2: BaseSequenceContent) => (NaiveVariantCaller_Reducer.combine(bsc1,bsc2)))

    //filter step
    val res: RDD[Pair[Int,BaseSequenceContent]] = baseSequenceContent.filter(
      record => NaiveVariantCaller_Filter.filterLowClarityAndReferenceMatchingBases(record)
    )

    //sort result
    val sortedRes = res.sortBy( record => (record._1) )

    //format and save output to file
    sortedRes.map(record => record._1 + "," + record._2).saveAsTextFile(outputPath)

    println("finished process..")
  }

  def getRecursiveListOfFilePaths(inputFolder: File): Seq[Path] = {
    val filePaths = new ListBuffer[Path]
    getRecursiveListOfFiles(inputFolder).foreach( f => filePaths += new Path(f.getAbsolutePath) );
    filePaths.toList
  }

  def getRecursiveListOfFiles(parentFolder: File): Array[File] = {
    val these = parentFolder.listFiles.filter(_.getName.toLowerCase.endsWith(".bam"))
    these ++ these.filter(_.isDirectory).flatMap(getRecursiveListOfFiles)
  }

  private def convertToSAMRecord(record: AlignmentRecord, recordConverter: AlignmentRecordConverter, sfhWritable: SAMFileHeaderWritable): SAMRecord = {
    recordConverter.convert(record, sfhWritable)
  }

}