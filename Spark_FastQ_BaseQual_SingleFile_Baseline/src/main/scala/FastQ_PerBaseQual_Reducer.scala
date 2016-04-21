package main.scala

import main.scala.utils.AvgCount

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 21.04.2016
  */
object FastQ_PerBaseQual_Reducer {

  def createAverageCount(qualVal: Int): AvgCount = {
    new AvgCount(qualVal,1)
  }

  def addAndCount(a: AvgCount, qualVal: Int): AvgCount = {
    a.addQualVal(qualVal)
  }

  def combine(a: AvgCount, b: AvgCount): AvgCount = {
    new AvgCount(a.getSumOfQualityValues()+b.getSumOfQualityValues(), a.getNumberOfQualityValues()+b.getNumberOfQualityValues())
  }

}