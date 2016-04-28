package utils

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 21.04.2016
  */
class JoinedResult(chrom: Int, pos: Int, id: String, ref: String, alt: String, qual: String, filter: String, info: String, format: String, genotypes: String) {
  private val delimiter = "\t"

  def getChrom(): Int = {
    chrom
  }

  def getPos(): Int = {
    pos
  }

  def getId(): String = {
    id
  }

  def getRef(): String = {
    ref
  }

  def getAlt(): String = {
    alt
  }

  def getQual(): String = {
    qual
  }

  def getFilter(): String = {
    filter
  }

  def getInfo(): String = {
    info
  }

  def getFormat(): String = {
    format
  }

  def getGenotypes(): String = {
    genotypes
  }

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder()
    sb.append(chrom)
      .append(delimiter)
      .append(pos)
      .append(delimiter)
      .append(id)
      .append(delimiter)
      .append(ref)
      .append(delimiter)
      .append(alt)
      .append(delimiter)
      .append(qual)
      .append(delimiter)
      .append(filter)
      .append(delimiter)
      .append(info.trim)
      .append(delimiter)
      .append(format)
      .append(delimiter)
      .append(genotypes.trim())
      .toString()
  }

}