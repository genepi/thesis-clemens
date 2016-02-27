package pigGene.storage.merged;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;

public class PigGeneInputFormat extends PigTextInputFormat {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		PigGeneRecordReader recordReader = new PigGeneRecordReader();
		try {
			recordReader.initialize(split, context);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return recordReader;
	}

}