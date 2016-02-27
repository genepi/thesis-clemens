package pigGene.storage.reference;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;

public class PigGeneInputFormatReferenceFile extends PigTextInputFormat {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(final InputSplit split, final TaskAttemptContext context) {
		final PigGeneRecordReaderReferenceFile recordReader = new PigGeneRecordReaderReferenceFile();
		try {
			recordReader.initialize(split, context);
		} catch (final IOException e) {
			e.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		}
		return recordReader;
	}

}