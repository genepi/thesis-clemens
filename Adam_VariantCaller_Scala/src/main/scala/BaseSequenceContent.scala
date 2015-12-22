package main.scala

import scala.collection.mutable.Map

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 26.11.15.
  */
@SerialVersionUID(9147195128784384417L)
class BaseSequenceContent(base: Char) extends Serializable {
  private val BASE_A: Char = 'A'
  private val BASE_C: Char = 'C'
  private val BASE_G: Char = 'G'
  private val BASE_T: Char = 'T'

  private var noOfBaseA: Int = 0
  private var noOfBaseC: Int = 0
  private var noOfBaseG: Int = 0
  private var noOfBaseT: Int = 0

  private var mostDominantBase: Char = ' '

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

  def setMostDominantBase(mostDominantBase: Char) {
    this.mostDominantBase = mostDominantBase
  }

  def getMostDominantBase() : Char = {
    this.mostDominantBase
  }

  def getPercentageOfBaseOccurrences(): Map[Character, Double] = {
    val result: Map[Character,Double] = Map[Character,Double]()
    val totalNoOfBases: Int = noOfBaseA + noOfBaseC + noOfBaseG + noOfBaseT
    val percentageValueBaseA: Double = calculatePercentageValue(noOfBaseA, totalNoOfBases)
    val percentageValueBaseC: Double = calculatePercentageValue(noOfBaseC, totalNoOfBases)
    val percentageValueBaseG: Double = calculatePercentageValue(noOfBaseG, totalNoOfBases)
    val percentageValueBaseT: Double = calculatePercentageValue(noOfBaseT, totalNoOfBases)

    result(BASE_A) = percentageValueBaseA
    result(BASE_C) = percentageValueBaseC
    result(BASE_G) = percentageValueBaseG
    result(BASE_T) = percentageValueBaseT

    result
  }

  private def calculatePercentageValue(baseOccurrence: Int, totalNoOfBases: Int): Double = {
    return (baseOccurrence * 100).toDouble / totalNoOfBases.toDouble
  }

  override def toString: String = String.valueOf(mostDominantBase)
}