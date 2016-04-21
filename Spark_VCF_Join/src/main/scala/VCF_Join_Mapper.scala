package main.scala

import java.util.Collections

import htsjdk.tribble.util.ParsingUtils
import htsjdk.variant.variantcontext.{Allele, Genotype, GenotypesContext, VariantContext}
import utils.JoinedResult

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 21.04.2016
  */
object VCF_Join_Mapper {

  def mapKeyBy(vcfRecord: VariantContext): Pair[Int, Int] = {
    (vcfRecord.getContig.toInt, vcfRecord.getStart)
  }

  def constructResult(key: Pair[Int, Int], value: Pair[VariantContext, Option[VariantContext]]): JoinedResult = {
    val leftTuple: VariantContext = value._1
    val alleles: java.util.List[Allele] = ParsingUtils.sortList(leftTuple.getAlleles)
    val ref: String = alleles.get(0).getBaseString
    val alt: String = alleles.get(1).getBaseString
    val format: String = "GT"

    return new JoinedResult(
      leftTuple.getContig.toInt,
      leftTuple.getStart,
      if (leftTuple.hasID) leftTuple.getID else ".",
      ref,
      alt,
      if (leftTuple.hasLog10PError()) leftTuple.getPhredScaledQual().toString else ".",
      this.filtersToString(leftTuple.getCommonInfo.getFilters),
      this.attributesToSortedString(leftTuple.getAttributes),
      format,
      this.genotypesToString(leftTuple.getGenotypes, ref, leftTuple.getAlternateAlleles),
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

  private def genotypesToString(genotypes: GenotypesContext, ref: String, altAlleles: java.util.List[Allele]): String = {
    val sb: StringBuilder = new StringBuilder
    val delimiter: Char = '\t'
    for (genotype <- genotypes) {
      if (genotype.getPloidy == 0) {
        sb.append("NA")
      }
      val separator: String = if (genotype.isPhased) "|" else "/"
      val al: ListBuffer[String] = new ListBuffer[String]()
      var base: String = null
      for (a <- genotype.getAlleles) {
        base = a.getBaseString
        if (base == ref) { //matches reference
          al.add(0.toString)
        }
        else {
          for (i <- 0 until altAlleles.size) {
            if (base == altAlleles.get(i).getBaseString) {
              al.add((i+1).toString)
            }
          }
        }
      }
      sb.append(ParsingUtils.join(separator, al))
      sb.append(delimiter)
    }
    return sb.toString
  }

  private def getReferenceAttributes(referenceTupleOption: Option[VariantContext]): String = {
    var rightAttr = " ";
    if (referenceTupleOption != None) {
      val rightTuple = referenceTupleOption.get
      rightAttr = this.attributesToSortedString(rightTuple.getAttributes())
    }
    rightAttr
  }

  private def attributesToSortedString(att: java.util.Map[String, AnyRef]): String = {
    val t: java.util.List[String] = new java.util.ArrayList[String](att.keySet)
    Collections.sort(t)
    val pairs: java.util.List[String] = new java.util.ArrayList[String]
    for (k <- t) {
      pairs.add(k + "=" + att.get(k));
    }
    val strings: Array[String] = pairs.toArray(new Array[String](pairs.size))
    this.join(";", strings, 0, strings.length)
  }

  private def join(separator: String, strings: Array[String], start: Int, end: Int): String = {
    if ((end - start) == 0) {
      return ""
    }
    val ret : StringBuilder = new StringBuilder
    ret.append(strings(start))
    var i: Int = start + 1
    while (i < end) {
      ret.append(separator)
      ret.append(strings(i))
      i += 1
    }
    ret.toString()
  }

}