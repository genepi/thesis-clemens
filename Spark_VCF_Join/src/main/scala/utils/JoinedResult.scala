package utils

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 21.04.2016
  */
class JoinedResult(chrom: Int, pos: Int, id: String, ref: String, alt: String, qual: String, filter: String, info: String, format: String, genotypes: String, infoRef: String) {
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
    if (infoRef != " ") {
      sb.append(';')
        .append(infoRef)
    }
    sb.append(delimiter)
      .append(format)
      .append(delimiter)
      .append(genotypes.trim())
      .toString()
  }

}