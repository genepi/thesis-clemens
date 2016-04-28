package main.scala

import java.util.Collections

import htsjdk.tribble.util.ParsingUtils
import htsjdk.variant.variantcontext.{Allele, GenotypesContext, VariantContext}
import utils.JoinedResult

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 21.04.2016
  */
object VCF_Join_Mapper {

  def mapToDataObject(value: VariantContext): JoinedResult = {
    val alleles: java.util.List[Allele] = ParsingUtils.sortList(value.getAlleles)
    val alternateAlleles: java.util.List[Allele] = value.getAlternateAlleles

    val chrom: Int = Integer.valueOf(value.getContig)
    val pos: Int = value.getStart
    val id: String = (if (value.hasID) value.getID else ".")
    val ref: String = alleles.get(0).getBaseString
    val alt: String = combineAlleles(alternateAlleles)
    val qual: String = if (value.hasLog10PError) String.valueOf(value.getPhredScaledQual) else "."
    val filter: String = this.filtersToString(value.getCommonInfo.getFilters)
    val info: String = this.attributesToSortedString(value.getAttributes)
    val format: String = "GT"
    var genotypes: String = ""
    if (value.hasGenotypes) {
      genotypes = this.genotypesToString(value.getGenotypes, ref, alternateAlleles)
    }
    return new JoinedResult(chrom, pos, id, ref, alt, qual, filter, info, format, genotypes)
  }

  def mapKeyBy(joinedRes: JoinedResult): Pair[Int, Int] = {
    (joinedRes.getChrom(), joinedRes.getPos())
  }

  def constructResult(key: Pair[Int, Int], value: Pair[JoinedResult, Option[JoinedResult]]): JoinedResult = {
    val leftTuple: JoinedResult = value._1
    val rightTuple = value._2

    if (rightTuple != None) {
      return new JoinedResult(
        leftTuple.getChrom,
        leftTuple.getPos,
        leftTuple.getId,
        leftTuple.getRef,
        leftTuple.getAlt,
        leftTuple.getQual,
        leftTuple.getFilter,
        leftTuple.getInfo + ";" + rightTuple.get.getInfo(),
        leftTuple.getFormat,
        leftTuple.getGenotypes
      )
    }

    return leftTuple
  }

  private def combineAlleles(alleles: java.util.List[Allele]): String = {
    val sb: StringBuilder = new StringBuilder
    sb.append(alleles.get(0).getBaseString)
    for (i <- 1 until alleles.size) {
      sb.append(",")
      sb.append(alleles.get(i).getBaseString)
    }
    return sb.toString
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