package main.scala.PerBaseSequenceContent

import org.apache.hadoop.io.Text
import org.seqdoop.hadoop_bam.SequencedFragment

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

}
