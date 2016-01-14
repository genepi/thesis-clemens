package main.scala.PerBaseQual

import org.seqdoop.hadoop_bam.SequencedFragment
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.io.Text

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 02.01.16.
  */
object FastQ_PerBaseQual_Mapper {
  private val OFFSET: Int = 33

  def flatMap(record: SequencedFragment): TraversableOnce[Pair[Int,Double]] = {
    val resList = new ListBuffer[Pair[Int,Double]]()
    val quality: Text = record.getQuality
    for(i <- 0 until quality.getLength) {
      resList.append(new Pair((i+1), getCorrespoindingIntValue(quality.charAt(i))))
    }
    resList.toTraversable
  }

  private def getCorrespoindingIntValue(qualVal: Int): Double = {
    (qualVal - OFFSET)
  }

}
