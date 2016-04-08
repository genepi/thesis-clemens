package main.scala

import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rich.RichVariant

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 26.03.16.
  */
object VCF_Join_Mapper {

  def mapKeyBy(variant: RichVariant): Pair[Int, Int] = {

    //TODO is the chrom encoded as string in AlignmentRecord
    //TODO can it simply be casted to int??

    (variant.getContig.getContigName.toInt, variant.getStart.toInt)

  }

}
