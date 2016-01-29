package main.scala

import org.bdgenomics.formats.avro.AlignmentRecord

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 27.01.16.
  */
object NaiveVariantCaller_Filter {
  private val MIN_BASE_QUAL: Int = 30
  private val MIN_MAP_QUAL: Int = 30
  private val MIN_ALIGN_QUAL: Int = 30
  private val MIN_READ_LENGTH: Int = 25
  private val MIN_CLARITY_PERCENTAGE: Int = 75

  def readFullfillsRequirements(alignmentRecord: AlignmentRecord): Boolean = {
    if (mappingQualitySufficient(alignmentRecord.getMapq)) {
      return true
    }
    return false
  }

  private def mappingQualitySufficient(mapQual: Int): Boolean = {
    // 255 means that mapping quality is not available
    if (mapQual >= MIN_MAP_QUAL || mapQual == 255) {
      return true
    }
    return false
  }

}
