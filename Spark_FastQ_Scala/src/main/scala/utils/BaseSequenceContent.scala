package main.scala.utils

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 15.01.16.
  */
@SerialVersionUID(1747425128466589429L)
class BaseSequenceContent(val percentages: Map[Char,Double]) extends Serializable {
  private val BASE_A: Char = 'A'
  private val BASE_C: Char = 'C'
  private val BASE_G: Char = 'G'
  private val BASE_T: Char = 'T'

  def canEqual(other: Any): Boolean = other.isInstanceOf[BaseSequenceContent]

  override def equals(other: Any): Boolean = other match {
    case that: BaseSequenceContent =>
      (that canEqual this) &&
        percentages == that.percentages
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(percentages)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = {
    val tabulator: String = "\t"
    this.percentages.get(BASE_A).get + tabulator + this.percentages.get(BASE_C).get + tabulator + this.percentages.get(BASE_G).get + tabulator + this.percentages.get(BASE_T).get
  }

}
