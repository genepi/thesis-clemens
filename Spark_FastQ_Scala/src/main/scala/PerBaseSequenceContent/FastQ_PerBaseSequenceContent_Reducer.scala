package main.scala.PerBaseSequenceContent

import main.scala.utils.BaseSequenceContent

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 02.01.16.
  */
object FastQ_PerBaseSequenceContent_Reducer {

  def createBaseSeqContent(base: Char): BaseSequenceContent = {
    new BaseSequenceContent(base)
  }

  def countAndCalculateBasePercentage(bsc: BaseSequenceContent, base: Char): BaseSequenceContent = {
    bsc.incrementBaseCount(base)
    bsc
  }

  def combine(a: BaseSequenceContent, b: BaseSequenceContent): BaseSequenceContent = {
    a.incrementNoOfBaseABy(b.getNoOfBaseA)
    a.incrementNoOfBaseCBy(b.getNoOfBaseC)
    a.incrementNoOfBaseGBy(b.getNoOfBaseG)
    a.incrementNoOfBaseTBy(b.getNoOfBaseT)
    a
  }

}