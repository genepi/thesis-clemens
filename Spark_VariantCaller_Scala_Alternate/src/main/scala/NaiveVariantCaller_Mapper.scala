package main.scala

import java.nio.charset.StandardCharsets

import htsjdk.samtools.SAMRecord

import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 29.01.16.
  */
object NaiveVariantCaller_Mapper {
  private val BASE_A: Char = 'A'
  private val BASE_C: Char = 'C'
  private val BASE_G: Char = 'G'
  private val BASE_T: Char = 'T'
  private val BASE_UNKNOWN: Char = 'U'

  private val MIN_CLARITY_PERCENTAGE: Int = 75

  def flatMap(sampleIdentifier:String, samRecord:SAMRecord): TraversableOnce[Pair[Pair[String, Int], Char]] = {
    val sequence: String = new String(samRecord.getReadBases(), StandardCharsets.UTF_8)
    val resList = new ListBuffer[Pair[Pair[String, Int], Char]]()

    val limit = sequence.length()-1
    for ( i <- 0 to limit) {
      if (NaiveVariantCaller_Filter.baseQualitySufficient(samRecord.getBaseQualities()(i))) {
        resList.append(
          new Pair(
            new Pair(
              sampleIdentifier,
              samRecord.getReferencePositionAtReadPosition(i+1)
              ),
            sequence.charAt(i)
          )
        )
      }
    }
    // TODO remove println
//    println("SIZE OF THE LISTBUFFER: " + resList.size)
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