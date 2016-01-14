package main.scala.utils

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 02.01.16.
  */
@SerialVersionUID(7147395128485584427L)
class AvgCount(var total: Double, var num: Int) extends Serializable {

  def avg(): Double = {
    (total / num)
  }

  def getTotal(): Double = {
    total
  }

  def getNum(): Int = {
    num
  }

  def incrementTotalBy(value: Double): Unit = {
    total = total + value
  }

  def incrementNumBy(value: Int): Unit = {
    num = num + value
  }

  override def toString: String = (total / num).toString

}