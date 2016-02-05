package main.scala

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 29.01.16.
  */
class BaseSequenceContent(base: Char) {
  private val BASE_A: Char = 'A'
  private val BASE_C: Char = 'C'
  private val BASE_G: Char = 'G'
  private val BASE_T: Char = 'T'

  private var noOfBaseA: Int = 0
  private var noOfBaseC: Int = 0
  private var noOfBaseG: Int = 0
  private var noOfBaseT: Int = 0

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

}