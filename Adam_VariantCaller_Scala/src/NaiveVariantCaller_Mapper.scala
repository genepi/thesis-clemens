import java.nio.charset.StandardCharsets

import htsjdk.samtools.SAMRecord

import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 04.12.15.
  */
object NaiveVariantCaller_Mapper {
  private val BASE_A: Char = 'A'
  private val BASE_C: Char = 'C'
  private val BASE_G: Char = 'G'
  private val BASE_T: Char = 'T'

  def flatMap(samRecord: SAMRecord): TraversableOnce[Pair[Int,Char]] = {
    val readBases: Array[Byte] = samRecord.getReadBases()
    val sequence: String = new String(readBases, StandardCharsets.UTF_8)
    val resList = new ListBuffer[Pair[Int,Char]]()



    //TODO basequality not available in the record...?!

    //TODO why is there a character 'N' occurring ??

    if (NaiveVariantCaller_Filter.readFullfillsRequirements(samRecord)) {
//      println("samrecordbasequal length: " + samRecord.getBaseQualities().length)
      for ( i <- 0 to sequence.length-1) {
//        if (NaiveVariantCaller_Filter.baseQualitySufficient(samRecord.getBaseQualities()(i))) {
          val outputKey: Int = samRecord.getReferencePositionAtReadPosition(i+1)
          val base = sequence.charAt(i) match {
            case BASE_A => resList.append(new Pair(outputKey, BASE_A))
            case BASE_C => resList.append(new Pair(outputKey, BASE_C))
            case BASE_G => resList.append(new Pair(outputKey, BASE_G))
            case BASE_T => resList.append(new Pair(outputKey, BASE_T))
            case default => println("base character '" + default + "' occurred")
          }
        }
      }
//    }
    resList.toTraversable
  }

}
