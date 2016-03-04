package main.scala

import org.bdgenomics.formats.avro.AlignmentRecord

import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 05.02.16.
  */
object FastQ_PerBaseQual_Mapper {
  private val OFFSET: Int = 33

  def flatMap(record: AlignmentRecord): TraversableOnce[Pair[Int, Int]] = {
    val resList = new ListBuffer[Pair[Int, Int]]()

    val quality: String = record.getQual
    for(i <- 0 until quality.length) {
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