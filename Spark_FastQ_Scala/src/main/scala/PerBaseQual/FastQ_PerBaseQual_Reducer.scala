package main.scala.PerBaseQual

import main.scala.utils.AvgCount

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 02.01.16.
  */
object FastQ_PerBaseQual_Reducer {

  def createAverageCount(value: Double): AvgCount = {
    new AvgCount(value,1)
  }

  def addAndCount(a: AvgCount, x: Double): AvgCount = {
    a.incrementTotalBy(x)
    a.incrementNumBy(1)
    a
  }

  def combine(a: AvgCount, b: AvgCount): AvgCount = {
    a.incrementTotalBy(b.getTotal())
    a.incrementNumBy(b.getNum())
    a
  }

}
