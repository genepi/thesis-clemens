package main.scala.join

import htsjdk.variant.variantcontext.VariantContext

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 13.02.16.
  */
object VCF_Join_Mapper {

  def mapKeyBy(vcfRecord: VariantContext): Pair[Int, Int] = {
    (vcfRecord.getContig.toInt, vcfRecord.getStart)
  }

}