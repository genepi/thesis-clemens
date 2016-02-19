package main.scala.join.utils

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 19.02.16.
  */
class JoinedResult(chrom: Int, pos: Int, id: String, ref: Char, alt: Char, qual: Int, filter: String, info: String) {
  private val delimiter = "\t"

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
      .append(info)
    sb.toString()
  }

}