package main.scala

import org.bdgenomics.formats.avro.AlignmentRecord

import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 05.02.16.
  */
object NaiveVariantCaller_Mapper {
  private val BASE_A: Char = 'A'
  private val BASE_C: Char = 'C'
  private val BASE_G: Char = 'G'
  private val BASE_T: Char = 'T'

  def flatMap(sampleIdentifier:Int, record:AlignmentRecord): TraversableOnce[Pair[NaiveVariantCallerKey,Char]] = {
    val resList = new ListBuffer[Pair[NaiveVariantCallerKey,Char]]()

    //TODO replace this code...
//    val samRecord: SAMRecord = convertToSAMRecord(record, recordConverter, sfhWritable)
//    val readBases: Array[Byte] = samRecord.getReadBases()
//    val sequence: String = new String(readBases, StandardCharsets.UTF_8)
//
//    if (NaiveVariantCaller_Filter.readFullfillsRequirements(samRecord)) {
//      for ( i <- 0 to sequence.length-1) {
//        if (!samRecord.getBaseQualityString.equals("*") && NaiveVariantCaller_Filter.baseQualitySufficient(samRecord.getBaseQualityString.charAt(i))) {
//          val outputKey: NaiveVariantCallerKey = new NaiveVariantCallerKey(sampleIdentifier, samRecord.getReferencePositionAtReadPosition(i+1))
//          sequence.charAt(i) match {
//            case BASE_A => resList.append(new Pair(outputKey, BASE_A))
//            case BASE_C => resList.append(new Pair(outputKey, BASE_C))
//            case BASE_G => resList.append(new Pair(outputKey, BASE_G))
//            case BASE_T => resList.append(new Pair(outputKey, BASE_T))
//            case default => println("base character '" + default + "' occurred at position " + outputKey)
//          }
//        }
//      }
//    }

//    val sequence: String = record.getSequence
//    for (i <- 0 until sequence.length) {
//
//      //TODO
////      if (!samRecord.getBaseQualityString.equals("*") && NaiveVariantCaller_Filter.baseQualitySufficient(samRecord.getBaseQualityString.charAt(i)))
//
//      record.mismatchingPositions
//
//
//      val outputKey: NaiveVariantCallerKey = new NaiveVariantCallerKey(sampleIdentifier, record.getReferencePositionAtReadPosition(i+1))
//      sequence.charAt(i) match {
//        case BASE_A => resList.append(new Pair(outputKey, BASE_A))
//        case BASE_C => resList.append(new Pair(outputKey, BASE_C))
//        case BASE_G => resList.append(new Pair(outputKey, BASE_G))
//        case BASE_T => resList.append(new Pair(outputKey, BASE_T))
//        case default => println("base character '" + default + "' occurred at position " + outputKey)
//      }
//    }
    resList.toTraversable
  }

}