package main.scala

import org.bdgenomics.formats.avro.AlignmentRecord

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 05.02.16.
  */
object FastQ_PerSequenceQual_Mapper {
  private val OFFSET: Int = 33

  def map(record: AlignmentRecord): Pair[Int,Int] = {
    new Pair(getMeanValue(record.getQual), 1)
  }

  private def getMeanValue(quality: String): Int = {
    var sumOfQualityValues: Int = 0
    for (qualVal <- quality.toCharArray) {
      sumOfQualityValues += getCorrespondingIntValue(qualVal)
    }
    return (Math.round(sumOfQualityValues.toDouble / quality.length.toDouble).toInt);
  }

  private def getCorrespondingIntValue(qualVal: Char): Int = {
    qualVal.toInt - OFFSET
  }

}