package main.scala.PerSequenceQual

import org.apache.hadoop.io.Text
import org.seqdoop.hadoop_bam.SequencedFragment

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 02.01.16.
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
    return (sumOfQualityValues / quality.getLength);
  }

  private def getCorrespondingIntValue(qualVal: Char): Int = {
    qualVal.toInt - OFFSET
  }

}