package main.scala

import java.io.FileNotFoundException

import htsjdk.samtools.SAMFileHeader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.LongWritable
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{RecordGroupDictionary, SequenceDictionary}
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.{AlignmentRecord, RecordGroupMetadata}
import org.bdgenomics.utils.misc.HadoopUtil
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, FileVirtualSplit, SAMRecordWritable}

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of InnsbruckØ
  * Created 05.02.16.
  */
object NaiveVariantCaller_Job {

  private def adamBamDictionaryLoad(samHeader: SAMFileHeader): SequenceDictionary = {
    SequenceDictionary(samHeader)
  }

  private def adamBamLoadReadGroups(samHeader: SAMFileHeader): RecordGroupDictionary = {
    RecordGroupDictionary.fromSAMHeader(samHeader)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("usage: spark-submit Adam_VariantCaller_Native_Scala-1.0-SNAPSHOT.jar <bam input directory> <output dir>")
      return;
    }

    val input = args(0)
    val output = args(1)

    val conf = new SparkConf()
    conf.setAppName("Adam_VariantCaller_Native_Scala")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator","org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
    conf.registerKryoClasses(Array(classOf[BaseSequenceContent], classOf[RecordGroupMetadata]))
    val sc = new SparkContext(conf)

    val configuration = new Configuration()
    configuration.set("mapreduce.input.fileinputformat.inputdir", input)


    //copied from ADAM code ...

    val path = new Path(input)
    val fs =
      Option(
        FileSystem.get(path.toUri, sc.hadoopConfiguration)
      ).getOrElse(
        throw new FileNotFoundException(
          s"Couldn't find filesystem for ${path.toUri} with Hadoop configuration ${sc.hadoopConfiguration}"
        )
      )

    val bamFiles =
      Option(
        if (fs.isDirectory(path)) fs.listStatus(path) else fs.globStatus(path)
      ).getOrElse(
        throw new FileNotFoundException(
          s"Couldn't find any files matching ${path.toUri}"
        )
      )

    val (seqDict, readGroups) =
      bamFiles
        .map(fs => fs.getPath)
        .flatMap(fp => {
          try {
            val samHeader = SAMHeaderReader.readSAMHeaderFrom(fp, sc.hadoopConfiguration)
            val sd = adamBamDictionaryLoad(samHeader)
            val rg = adamBamLoadReadGroups(samHeader)
            Some((sd, rg))
          } catch {
            case e: Throwable => {
              None
            }
          }
        }).reduce((kv1, kv2) => {
        (kv1._1 ++ kv2._1, kv1._2 ++ kv2._2)
      })

    val job = HadoopUtil.newJob(sc)
    val bamInput = sc.newAPIHadoopFile(
      input,
      classOf[AnySAMInputFormat],
      classOf[LongWritable],
      classOf[SAMRecordWritable],
      ContextUtil.getConfiguration(job)
    )

    val hadoopRdd = bamInput.asInstanceOf[NewHadoopRDD[Void,SAMRecordWritable]]
    val myRdd: RDD[Pair[String, SAMRecordWritable]] = hadoopRdd.mapPartitionsWithInputSplit { (inputSplit, iterator) ⇒
      val file = inputSplit.asInstanceOf[FileVirtualSplit]
      iterator.map(record => (file.getPath.getName, record._2))
    }

    val adamRDD: RDD[Pair[String, AlignmentRecord]] = myRdd.map( record => NaiveVariantCaller_Mapper.mapSampleIdentifierWithConvertedInputObject(record, seqDict, readGroups) )

    val preFilter: RDD[Pair[String,AlignmentRecord]] = adamRDD.filter( record => NaiveVariantCaller_Filter.readFullfillsRequirements(record._2) )
    val richRecordMap: RDD[Pair[String,RichAlignmentRecord]] = preFilter.map( record => NaiveVariantCaller_Mapper.convertAlignmentRecordToRichAlignmentRecord(record._1, record._2) )
    val baseCount: RDD[Pair[Pair[String, Int], Char]] = richRecordMap.flatMap( record => NaiveVariantCaller_Mapper.flatMap(record._1, record._2) )

    //reduce step
    val baseSequenceContent: RDD[Pair[Pair[String, Int], BaseSequenceContent]] = baseCount.combineByKey(
      (base: Char) => (NaiveVariantCaller_Reducer.createBaseSeqContent(base)),
      (bsc: BaseSequenceContent, base: Char) => (NaiveVariantCaller_Reducer.countAndCalculateBasePercentage(bsc,base)),
      (bsc1: BaseSequenceContent, bsc2: BaseSequenceContent) => (NaiveVariantCaller_Reducer.combine(bsc1,bsc2)))

    //map most-dominant base
    val baseSequenceContentMapped: RDD[Pair[Pair[String, Int], Char]] = baseSequenceContent.mapValues( record => NaiveVariantCaller_Mapper.mapMostDominantBase(record) )

    //filter step
    val res: RDD[Pair[Pair[String, Int], Char]] = baseSequenceContentMapped.filter(
      record => NaiveVariantCaller_Filter.filterLowClarityAndReferenceMatchingBases(record._1._2, record._2)
    )

    //format and save output to file
    res.map(record => record._1._1 + "," + record._1._2 + "," + record._2).saveAsTextFile(output)
  }

}