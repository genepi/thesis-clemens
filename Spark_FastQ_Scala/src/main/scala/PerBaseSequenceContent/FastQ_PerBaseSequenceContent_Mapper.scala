package main.scala.PerBaseSequenceContent

import org.apache.hadoop.io.Text
import org.seqdoop.hadoop_bam.SequencedFragment
import main.scala.utils.BaseSequenceContent

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 02.01.16.
  */
object FastQ_PerBaseSequenceContent_Mapper {
  private val BASE_A: Char = 'A'
  private val BASE_C: Char = 'C'
  private val BASE_G: Char = 'G'
  private val BASE_T: Char = 'T'

  def flatMap(record: SequencedFragment): TraversableOnce[Pair[Int,Char]] = {
    val resList = new ListBuffer[Pair[Int,Char]]()
    val sequence : Text = record.getSequence()
    for( i <- 0 until sequence.getLength ) {
      val outputKey = i+1
      sequence.charAt(i) match {
        case BASE_A => resList.append(new Pair(outputKey, BASE_A))
        case BASE_C => resList.append(new Pair(outputKey, BASE_C))
        case BASE_G => resList.append(new Pair(outputKey, BASE_G))
        case BASE_T => resList.append(new Pair(outputKey, BASE_T))
        case default => println("base character '" + default + "' occurred at position " + outputKey)
      }
    }
    resList.toTraversable
  }

  def map(key: Int, values: Iterable[Char]) : Pair[Int,BaseSequenceContent] = {
    Pair(key,new BaseSequenceContent(getPercentageOfBaseOccurrences(values)))
  }

  private def getPercentageOfBaseOccurrences(values : Iterable[Char]): Map[Char,Double] = {
    val result: mutable.Map[Char,Double] = new mutable.HashMap[Char,Double]()

    var noOfBaseA: Int = 0
    var noOfBaseC: Int = 0
    var noOfBaseG: Int = 0
    var noOfBaseT: Int = 0

    for (base <- values) {
      base match {
        case BASE_A => noOfBaseA += 1
        case BASE_C => noOfBaseC += 1
        case BASE_G => noOfBaseG += 1
        case BASE_T => noOfBaseT += 1
      }
    }

    val totalNoOfBases: Int = noOfBaseA + noOfBaseC + noOfBaseG + noOfBaseT
    result.put(BASE_A, calculatePercentageValue(noOfBaseA, totalNoOfBases))
    result.put(BASE_C, calculatePercentageValue(noOfBaseC, totalNoOfBases))
    result.put(BASE_G, calculatePercentageValue(noOfBaseG, totalNoOfBases))
    result.put(BASE_T, calculatePercentageValue(noOfBaseT, totalNoOfBases))
    result.toMap
  }

  private def calculatePercentageValue(baseOccurrence: Int, totalNoOfBases: Int): Double = {
    (baseOccurrence * 100).toDouble / totalNoOfBases.toDouble
  }

}
