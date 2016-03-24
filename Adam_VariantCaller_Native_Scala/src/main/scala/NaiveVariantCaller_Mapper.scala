package main.scala

import org.apache.hadoop.io.Text
import org.bdgenomics.adam.converters.SAMRecordConverter
import org.bdgenomics.adam.models.{RecordGroupDictionary, SequenceDictionary}
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

  def flatMap(sampleIdentifier: String, record: AlignmentRecord): TraversableOnce[Pair[Pair[String, Int], Char]] = {
    val resList = new ListBuffer[Pair[Pair[String, Int], Char]]()

    //TODO replace this code...


//    val samRecord: SAMRecord = convertToSAMRecord(record, recordConverter, sfhWritable)
//    val readBases: Array[Byte] = samRecord.getReadBases()
//    val sequence: String = new String(readBases, StandardCharsets.UTF_8)
//
//    if (NaiveVariantCaller_Filter.readFullfillsRequirements(samRecord)) {
//      for ( i <- 0 to sequence.length-1) {
//        if (!samRecord.getBaseQualityString.equals("*") && NaiveVariantCaller_Filter.baseQualitySufficient(samRecord.getBaseQualityString.charAt(i))) {
//          val outputKey: NaiveVariantCallerKey = new NaiveVariantCallerKey(sampleIdentifier, samRecord.getReferencePositionAtReadPosition(i+1))
//          sequence.charAt(i) match {
//            case BASE_A => resList.append(new Pair(outputKey, BASE_A))
//            case BASE_C => resList.append(new Pair(outputKey, BASE_C))
//            case BASE_G => resList.append(new Pair(outputKey, BASE_G))
//            case BASE_T => resList.append(new Pair(outputKey, BASE_T))
//            case default => println("base character '" + default + "' occurred at position " + outputKey)
//          }
//        }
//      }
//    }

//    val sequence: String = record.getSequence
//    for (i <- 0 until sequence.length) {
//
//      //TODO
////      if (!samRecord.getBaseQualityString.equals("*") && NaiveVariantCaller_Filter.baseQualitySufficient(samRecord.getBaseQualityString.charAt(i)))
//
//      record.mismatchingPositions
//
//
//      val outputKey: NaiveVariantCallerKey = new NaiveVariantCallerKey(sampleIdentifier, record.getReferencePositionAtReadPosition(i+1))
//      sequence.charAt(i) match {
//        case BASE_A => resList.append(new Pair(outputKey, BASE_A))
//        case BASE_C => resList.append(new Pair(outputKey, BASE_C))
//        case BASE_G => resList.append(new Pair(outputKey, BASE_G))
//        case BASE_T => resList.append(new Pair(outputKey, BASE_T))
//        case default => println("base character '" + default + "' occurred at position " + outputKey)
//      }
//    }
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