package main.scala.utils

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 21.04.2016
  */
class AvgCount(var sumOfQualityValues: Int = 0, var numberOfQualityValues: Int = 0) {

  def addQualVal(qualVal: Int): AvgCount = {
    this.sumOfQualityValues += qualVal
    this.numberOfQualityValues += 1
    this
  }

  def getSumOfQualityValues(): Int = {
    this.sumOfQualityValues
  }

  def getNumberOfQualityValues(): Int = {
    this.numberOfQualityValues
  }

  def getMeanValue(): Double = {
    (sumOfQualityValues.toDouble / numberOfQualityValues.toDouble)
  }

}