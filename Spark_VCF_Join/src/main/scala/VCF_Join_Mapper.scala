package main.scala

import htsjdk.tribble.util.ParsingUtils
import htsjdk.variant.variantcontext.{Allele, VariantContext}
import utils.JoinedResult

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
    val leftTuple: VariantContext = value._1
    val rightTupleOption: Option[VariantContext] = value._2

    val alleles = ParsingUtils.sortList(leftTuple.getAlleles)
    val leftAttr = ParsingUtils.sortedString(leftTuple.getAttributes())

    if (rightTupleOption == None) { //only use left part to construct result

      return new JoinedResult(
        leftTuple.getContig.toInt,
        leftTuple.getStart,
        if (leftTuple.hasID) leftTuple.getID else ".",

        alleles.get(0).toString.charAt(0),
        alleles.get(1).toString.charAt(0),
        if (leftTuple.hasLog10PError()) leftTuple.getPhredScaledQual().toString else ".",

        leftTuple.getFilters.toString,
        leftAttr
      )
    }

    val rightTuple = rightTupleOption.get
    val rightAttr = ParsingUtils.sortedString(rightTuple.getAttributes())

    return new JoinedResult(
      leftTuple.getContig.toInt,
      leftTuple.getStart,
      rightTuple.getID,

      alleles.get(0).toString.charAt(0),
      alleles.get(1).toString.charAt(0),
      if (leftTuple.hasLog10PError()) leftTuple.getPhredScaledQual().toString else ".",

      leftTuple.getFilters.toString,
      (leftAttr + rightAttr)
    )


  }

}