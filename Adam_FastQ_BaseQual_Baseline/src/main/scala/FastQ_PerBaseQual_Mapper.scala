package main.scala

import org.apache.hadoop.io.Text
import org.bdgenomics.formats.avro.AlignmentRecord

import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 05.02.16.
  */
object FastQ_PerBaseQual_Mapper {
  private val OFFSET: Int = 33

  def mapSampleIdentifierWithConvertedInputObject(record: Pair[String, Text]) : Pair[String, AlignmentRecord] = {
    new Pair(record._1, CustomFastqRecordConverter.convertRead(record))
  }

  def flatMap(sampleIdentifier: String, record: AlignmentRecord): TraversableOnce[Pair[Pair[String, Int], Int]] = {
    val resList = new ListBuffer[Pair[Pair[String, Int], Int]]()

    val quality: String = record.getQual
    for(i <- 0 until quality.length) {
      resList.append(new Pair(new Pair(sampleIdentifier, (i+1)), getCorrespoindingIntValue(quality.charAt(i))))
    }
    resList.toTraversable
  }

  def map(pair: Pair[Pair[String, Int], AvgCount]): Pair[Pair[String, Int], Double] = {
    new Pair(pair._1, pair._2.getMeanValue())
  }

  private def getCorrespoindingIntValue(qualVal: Int): Int = {
    (qualVal - OFFSET)
  }

}