package main.scala

import main.utils.AvgCount
import org.bdgenomics.formats.avro.AlignmentRecord

import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 05.02.16.
  */
object FastQ_PerBaseQual_Mapper {
  private val OFFSET: Int = 33

  def flatMap(sampleIdentifier:Int, record: AlignmentRecord): TraversableOnce[Pair[Pair[Int, Int], Int]] = {
    val resList = new ListBuffer[Pair[Pair[Int, Int], Int]]()

    //TODO delete replaced line of code...
//    val quality: Text = record.getQuality

    val quality: String = record.getQual
    for(i <- 0 until quality.length) {
      resList.append(new Pair(new Pair(sampleIdentifier, (i+1)), getCorrespoindingIntValue(quality.charAt(i))))
    }
    resList.toTraversable
  }

  def map(pair: Pair[Pair[Int, Int], AvgCount]): Pair[Pair[Int, Int], Double] = {
    new Pair(pair._1, pair._2.getMeanValue())
  }

  private def getCorrespoindingIntValue(qualVal: Int): Int = {
    (qualVal - OFFSET)
  }

}