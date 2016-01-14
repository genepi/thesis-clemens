package main.scala.utils

import scala.collection.mutable.Map

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 26.11.15.
  */
@SerialVersionUID(9147195128784384407L)
class BaseSequenceContent(base: Char) extends Serializable {
  private val BASE_A: Char = 'A'
  private val BASE_C: Char = 'C'
  private val BASE_G: Char = 'G'
  private val BASE_T: Char = 'T'

  private var noOfBaseA: Int = 0
  private var noOfBaseC: Int = 0
  private var noOfBaseG: Int = 0
  private var noOfBaseT: Int = 0

  private var percentageBaseA: Double = 0
  private var percentageBaseC: Double = 0
  private var percentageBaseG: Double = 0
  private var percentageBaseT: Double = 0

  def incrementBaseCount(base: Char): BaseSequenceContent = {
    base match {
      case BASE_A => noOfBaseA += 1
      case BASE_C => noOfBaseC += 1
      case BASE_G => noOfBaseG += 1
      case BASE_T => noOfBaseT += 1
    }
    this
  }

  def incrementNoOfBaseABy(value: Int): BaseSequenceContent = {
    noOfBaseA += value
    this
  }

  def incrementNoOfBaseCBy(value: Int): BaseSequenceContent = {
    noOfBaseC += value
    this
  }

  def incrementNoOfBaseGBy(value: Int): BaseSequenceContent = {
    noOfBaseG += value
    this
  }

  def incrementNoOfBaseTBy(value: Int): BaseSequenceContent = {
    noOfBaseT += value
    this
  }

  def getNoOfBaseA: Int = {
    return noOfBaseA
  }

  def getNoOfBaseC: Int = {
    return noOfBaseC
  }

  def getNoOfBaseG: Int = {
    return noOfBaseG
  }

  def getNoOfBaseT: Int = {
    return noOfBaseT
  }

  def calculatePercentageOfBaseOccurrences(): Unit = {
    val totalNoOfBases: Int = noOfBaseA + noOfBaseC + noOfBaseG + noOfBaseT

    this.percentageBaseA = calculatePercentageValue(noOfBaseA, totalNoOfBases)
    this.percentageBaseC = calculatePercentageValue(noOfBaseC, totalNoOfBases)
    this.percentageBaseG = calculatePercentageValue(noOfBaseG, totalNoOfBases)
    this.percentageBaseT = calculatePercentageValue(noOfBaseT, totalNoOfBases)
  }

  private def calculatePercentageValue(baseOccurrence: Int, totalNoOfBases: Int): Double = {
    return (baseOccurrence * 100).toDouble / totalNoOfBases.toDouble
  }

  override def toString: String =  {
    val tab: String = "\t"
    this.calculatePercentageOfBaseOccurrences()
    return this.percentageBaseA + tab + this.percentageBaseC + tab + this.percentageBaseG + tab + this.percentageBaseT
  }

}