package main.scala

import java.nio.charset.StandardCharsets
import htsjdk.samtools.SAMRecord
import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 22.01.16.
  */
object NaiveVariantCaller_Mapper {
  private val BASE_A: Char = 'A'
  private val BASE_C: Char = 'C'
  private val BASE_G: Char = 'G'
  private val BASE_T: Char = 'T'

  private val MIN_BASE_QUAL: Int = 30

  def flatMap(sampleIdentifier:String, samRecord:SAMRecord): TraversableOnce[Pair[NaiveVariantCallerKey,Char]] = {
    val sequence: String = new String(samRecord.getReadBases(), StandardCharsets.UTF_8)
    val baseQualities = samRecord.getBaseQualities()
    val resList = new ListBuffer[Pair[NaiveVariantCallerKey,Char]]()

    for ( i <- 0 to sequence.length-1) {
      if (baseQualitySufficient(baseQualities(i))) {
        val outputKey: NaiveVariantCallerKey = new NaiveVariantCallerKey(sampleIdentifier, samRecord.getReferencePositionAtReadPosition(i+1))
        sequence.charAt(i) match {
          case BASE_A => resList.append(new Pair(outputKey, BASE_A))
          case BASE_C => resList.append(new Pair(outputKey, BASE_C))
          case BASE_G => resList.append(new Pair(outputKey, BASE_G))
          case BASE_T => resList.append(new Pair(outputKey, BASE_T))
          case default => println("base character '" + default + "' occurred at position " + outputKey)
        }
      }
    }
    resList.toTraversable
  }

  private def baseQualitySufficient(baseQual: Int): Boolean = {
    if (baseQual >= MIN_BASE_QUAL) {
      return true
    }
    return false
  }

}