package main.scala

import main.utils.AvgCount

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 04.02.16.
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