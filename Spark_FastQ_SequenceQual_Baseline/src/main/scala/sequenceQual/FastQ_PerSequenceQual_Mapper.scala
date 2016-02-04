package main.scala.sequenceQual

import org.apache.hadoop.io.Text
import org.seqdoop.hadoop_bam.SequencedFragment

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 04.02.16.
  */
object FastQ_PerSequenceQual_Mapper {
  private val OFFSET: Int = 33

  def map(record: SequencedFragment): Pair[Int,Int] = {
    new Pair(getMeanValue(record.getQuality), 1)
  }

  private def getMeanValue(quality: Text): Int = {
    var sumOfQualityValues: Int = 0
    for (qualVal <- quality.toString.toCharArray) {
      sumOfQualityValues += getCorrespondingIntValue(qualVal)
    }
    return (Math.round(sumOfQualityValues.toDouble / quality.getLength.toDouble).toInt);
  }

  private def getCorrespondingIntValue(qualVal: Char): Int = {
    qualVal.toInt - OFFSET
  }

}