package main.scala

import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.Text
import org.bdgenomics.formats.avro.AlignmentRecord

/**
  * master-thesis Clemens Banas
  * Organization: DBIS - University of Innsbruck
  * Created 24.03.16.
  */
object CustomFastqRecordConverter {

  /**
    * modified the signature compared to the original method from FastqRecordConverter.class
    * instead of passing an element (Void,Text) an element of type (String,Text) is injected
    *
    * @param element
    * @param recordGroupOpt
    * @param setFirstOfPair
    * @param setSecondOfPair
    * @param stringency
    * @return
    */
  def convertRead(element: (String, Text), recordGroupOpt: Option[String] = None, setFirstOfPair: Boolean = false, setSecondOfPair: Boolean = false, stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecord = {
    val lines = element._2.toString.split('\n')
    require(lines.length == 4, "Record has wrong format:\n" + element._2.toString)

    def trimTrailingReadNumber(readName: String): String = {
      if (readName.endsWith("/1")) {
        if (setSecondOfPair) {
          throw new Exception(
            s"Found read name $readName ending in '/1' despite second-of-pair flag being set"
          )
        }
        readName.dropRight(2)
      } else if (readName.endsWith("/2")) {
        if (setFirstOfPair) {
          throw new Exception(
            s"Found read name $readName ending in '/2' despite first-of-pair flag being set"
          )
        }
        readName.dropRight(2)
      } else {
        readName
      }
    }

    // get fields for first read in pair
    val readName = trimTrailingReadNumber(lines(0).drop(1))
    val readSequence = lines(1)

    lazy val suffix = s"\n=== printing received Fastq record for debugging ===\n${lines.mkString("\n")}\n=== End of debug output for Fastq record ==="
    if (stringency == ValidationStringency.STRICT && lines(3) == "*" && readSequence.length > 1)
      throw new IllegalArgumentException(s"Fastq quality must be defined. $suffix")
    else if (stringency == ValidationStringency.STRICT && lines(3).length != readSequence.length)
      throw new IllegalArgumentException(s"Fastq sequence and quality strings must have the same length.\n Fastq quality string of length ${lines(3).length}, expected ${readSequence.length} from the sequence length. $suffix")

    val readQualities =
      if (lines(3) == "*")
        "B" * readSequence.length
      else if (lines(3).length < lines(1).length)
        lines(3) + ("B" * (lines(1).length - lines(3).length))
      else if (lines(3).length > lines(1).length)
        throw new NotImplementedError("Not implemented")
      else
        lines(3)

    require(
      readSequence.length == readQualities.length,
      "Read " + readName + " has different sequence and qual length: " +
        "\n\tsequence=" + readSequence + "\n\tqual=" + readQualities
    )

    val builder = AlignmentRecord.newBuilder()
      .setReadName(readName)
      .setSequence(readSequence)
      .setQual(readQualities)
      .setReadPaired(setFirstOfPair || setSecondOfPair)
      .setProperPair(null)

      //      .setReadInFragment(
      //        if (setFirstOfPair) 0
      //        else if (setSecondOfPair) 1
      //        else null
      //      )
      .setReadNegativeStrand(null)
      .setMateNegativeStrand(null)
      .setPrimaryAlignment(null)
      .setSecondaryAlignment(null)
      .setSupplementaryAlignment(null)

    recordGroupOpt.foreach(builder.setRecordGroupName)

    builder.build()
  }

}
