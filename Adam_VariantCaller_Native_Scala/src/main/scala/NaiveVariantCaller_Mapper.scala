package main.scala

import org.bdgenomics.adam.converters.SAMRecordConverter
import org.bdgenomics.adam.models.{RecordGroupDictionary, SequenceDictionary}
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.AlignmentRecord
import org.seqdoop.hadoop_bam.SAMRecordWritable

import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 05.02.16.
  */
object NaiveVariantCaller_Mapper {
  private val BASE_A: Char = 'A'
  private val BASE_C: Char = 'C'
  private val BASE_G: Char = 'G'
  private val BASE_T: Char = 'T'
  private val BASE_UNKNOWN: Char = 'U'
  private val MIN_CLARITY_PERCENTAGE: Int = 75
  private val samRecordConverter = new SAMRecordConverter

  def mapSampleIdentifierWithConvertedInputObject(record: Pair[String, SAMRecordWritable], seqDict: SequenceDictionary, readGroups: RecordGroupDictionary) : Pair[String, AlignmentRecord] = {
    new Pair(record._1, samRecordConverter.convert(record._2.get(), seqDict, readGroups))
  }

  def convertAlignmentRecordToRichAlignmentRecord(sampleIdentifier: String, record: AlignmentRecord): Pair[String, RichAlignmentRecord] = {
    new Pair(sampleIdentifier, RichAlignmentRecord.apply(record))
  }

  def flatMap(sampleIdentifier: String, record: RichAlignmentRecord): TraversableOnce[Pair[Pair[String, Int], Char]] = {
    val resList = new ListBuffer[Pair[Pair[String, Int], Char]]()
    val sequence: String = record.getSequence

    val limit = sequence.length()-1
    for(i <- 0 to limit) {
      if (NaiveVariantCaller_Filter.baseQualitySufficient(record.qualityScores(i))) {
        if (record.readOffsetToReferencePosition(i) != None) {
          val refPos = record.readOffsetToReferencePosition(i).get.pos.toInt
          resList.append(
            new Pair(
              new Pair(
                sampleIdentifier,
                refPos
              ),
              sequence.charAt(i)
            )
          )
        }
      }
    }
    resList.toTraversable
  }

  def mapMostDominantBase(bsc: BaseSequenceContent): Char = {
    val totalNoOfBases: Int = bsc.getNoOfBaseA + bsc.getNoOfBaseC + bsc.getNoOfBaseG + bsc.getNoOfBaseT
    if (calculatePercentageValue(bsc.getNoOfBaseA, totalNoOfBases) >= MIN_CLARITY_PERCENTAGE) {
      return BASE_A
    }
    else if (calculatePercentageValue(bsc.getNoOfBaseC, totalNoOfBases) >= MIN_CLARITY_PERCENTAGE) {
      return BASE_C
    }
    else if (calculatePercentageValue(bsc.getNoOfBaseG, totalNoOfBases) >= MIN_CLARITY_PERCENTAGE) {
      return BASE_G
    }
    else if (calculatePercentageValue(bsc.getNoOfBaseT, totalNoOfBases) >= MIN_CLARITY_PERCENTAGE) {
      return BASE_T
    }
    else {
      return BASE_UNKNOWN
    }
  }

  private def calculatePercentageValue(baseOccurrence: Int, totalNoOfBases: Int): Double = {
    return (baseOccurrence * 100).toDouble / totalNoOfBases.toDouble
  }

}