package main.scala

import htsjdk.tribble.util.ParsingUtils
import htsjdk.variant.variantcontext.{Genotype, GenotypesContext, VariantContext}
import utils.JoinedResult
import scala.collection.JavaConversions._

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
    val alleles = ParsingUtils.sortList(leftTuple.getAlleles)

    return new JoinedResult(
      leftTuple.getContig.toInt,
      leftTuple.getStart,
      if (leftTuple.hasID) leftTuple.getID else ".",
      alleles.get(0).toString.charAt(0),
      alleles.get(1).toString.charAt(0),
      if (leftTuple.hasLog10PError()) leftTuple.getPhredScaledQual().toString else ".",
      this.filtersToString(leftTuple.getCommonInfo.getFilters),
      this.attributesToString(leftTuple.getAttributes),
      this.genotypesToString(leftTuple.getGenotypes),
      this.getReferenceAttributes(value._2)
    )
  }

  private def filtersToString(filters: java.util.Set[String]): String = {
    if (filters.isEmpty) {
      return "PASS"
    }
    val delimiter: Char = ';'
    val sb: StringBuilder = new StringBuilder
    for (filter <- filters) {
      sb.append(filter)
      sb.append(delimiter)
    }
    return sb.toString
  }

  private def attributesToString(att: java.util.Map[String, AnyRef]): String = {
    val delimiter: Char = ';'
    val equals: Char = '='
    val sb: StringBuilder = new StringBuilder
    for (entry <- att.entrySet) {
      sb.append(entry.getKey)
      sb.append(equals)
      sb.append(entry.getValue)
      sb.append(delimiter)
    }
    return sb.toString
  }

  private def genotypesToString(genotypes: GenotypesContext): String = {
    val sb: StringBuilder = new StringBuilder
    val delimiter: Char = ' ';
    for (genotype: Genotype <- genotypes) {
      sb.append(genotype.getGenotypeString)
      sb.append(delimiter);
    }
    return sb.toString()
  }

  private def getReferenceAttributes(referenceTupleOption: Option[VariantContext]): String = {
    var rightAttr = " ";
    if (referenceTupleOption != None) {
      val rightTuple = referenceTupleOption.get
      rightAttr =  this.attributesToString(rightTuple.getAttributes())
    }
    rightAttr
  }

}