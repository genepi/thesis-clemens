package main.scala

import org.bdgenomics.formats.avro.AlignmentRecord

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 26.03.16.
  */
object VCF_Join_Mapper {

  def mapKeyBy(record: AlignmentRecord): Pair[Int, Int] = {

    //TODO is the chrom encoded as string in AlignmentRecord
    //TODO can it simply be casted to int??

    (record.getContig.getContigName, record.getStart.toInt)
  }

}
