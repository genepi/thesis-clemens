package main.scala

import main.scala.utils.AvgCount
import org.apache.hadoop.io.Text
import org.seqdoop.hadoop_bam.SequencedFragment

import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 21.04.2016
  */
object FastQ_PerBaseQual_Mapper {
  private val OFFSET: Int = 33

  def flatMap(record: SequencedFragment): TraversableOnce[Pair[Int, Int]] = {
    val resList = new ListBuffer[Pair[Int, Int]]()
    val quality: Text = record.getQuality
    for(i <- 0 until quality.getLength) {
      resList.append(new Pair((i+1), getCorrespoindingIntValue(quality.charAt(i))))
    }
    resList.toTraversable
  }

  def map(pair: Pair[Int, AvgCount]): Pair[Int, Double] = {
    new Pair(pair._1, pair._2.getMeanValue())
  }

  private def getCorrespoindingIntValue(qualVal: Int): Int = {
    (qualVal - OFFSET)
  }

}