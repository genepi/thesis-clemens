package main.scala.join

import htsjdk.variant.variantcontext.VariantContext
import main.scala.join.utils.JoinedResult

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 13.02.16.
  */
object VCF_Join_Mapper {

  def mapKeyBy(vcfRecord: VariantContext): Pair[Int, Int] = {
    (vcfRecord.getContig.toInt, vcfRecord.getStart)
  }

  def constructResult(key: Pair[Int, Int], value: Pair[VariantContext, Option[VariantContext]]): JoinedResult = {

    //TODO construct result

    val leftTuple: VariantContext = value._1
    val rightTuple: Option[VariantContext] = value._2

    if (rightTuple == None) { //only use left part to construct result
      return new JoinedResult(
        key._1,
        key._2,
        ...
      )
    }

    // TODO else combine the two resultsets to form the joined result


  }

}