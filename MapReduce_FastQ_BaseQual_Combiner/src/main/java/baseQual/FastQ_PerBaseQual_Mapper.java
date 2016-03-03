package baseQual;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.SequencedFragment;
import utils.IdentifierPositionKeyWritable;
import utils.QualityCountHelperWritable;

import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 04.02.16.
 */
public class FastQ_PerBaseQual_Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, SequencedFragment, IdentifierPositionKeyWritable, QualityCountHelperWritable> {
    private static final int OFFSET = 33;

    @Override
    protected void map(Object key, SequencedFragment value, Context context) throws IOException, InterruptedException {
        final String sampleIdentifier = ((FileSplit)context.getInputSplit()).getPath().getName();

        final Text quality = value.getQuality();
        for (int i = 0; i < quality.getLength(); i++) {
            context.write(new IdentifierPositionKeyWritable(sampleIdentifier, (i + 1)), new QualityCountHelperWritable(getCorrespondingIntValue(quality.charAt(i))));
        }
    }

    private int getCorrespondingIntValue(int qualVal) {
        return qualVal - FastQ_PerBaseQual_Mapper.OFFSET;
    }

}