package main.scala

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 21.04.2016
  */
object NaiveVariantCaller_Reducer {

  def createBaseSeqContent(base: Char): BaseSequenceContent = {
    new BaseSequenceContent(base).incrementBaseCount(base)
  }

  def countAndCalculateBasePercentage(baseSeqContent: BaseSequenceContent, base: Char): BaseSequenceContent = {
    baseSeqContent.incrementBaseCount(base)
  }

  def combine(bsc1: BaseSequenceContent, bsc2: BaseSequenceContent): BaseSequenceContent = {
    bsc1.incrementNoOfBaseABy(bsc2.getNoOfBaseA)
    bsc1.incrementNoOfBaseCBy(bsc2.getNoOfBaseC)
    bsc1.incrementNoOfBaseGBy(bsc2.getNoOfBaseG)
    bsc1.incrementNoOfBaseTBy(bsc2.getNoOfBaseT)
  }

}
