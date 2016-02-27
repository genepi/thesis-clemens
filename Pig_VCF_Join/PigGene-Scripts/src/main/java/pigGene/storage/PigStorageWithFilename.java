package pigGene.storage;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * Extension of the PigStorage functionality that adds the filename as last
 * parameter to the tuple. Needed to be able to find out to which file each line
 * in a relation corresponds when multiple files are loaded from the same
 * directory.
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
public class PigStorageWithFilename extends PigStorage {
	private String filename = null;

	public PigStorageWithFilename(final String delimiter) {
		super(delimiter);
	}

	public PigStorageWithFilename() {
		this("\t");
	}

	@Override
	public void prepareToRead(@SuppressWarnings("rawtypes") final RecordReader reader, final PigSplit split) {
		super.prepareToRead(reader, split);
		final String path = ((FileSplit) split.getWrappedSplit()).getPath().toString();
		final int offset = path.lastIndexOf('/');
		filename = path.substring(offset + 1, path.length());
	}

	@Override
	public Tuple getNext() throws IOException {
		final BinSedesTuple myTuple = (BinSedesTuple) super.getNext();
		if (myTuple != null) {
			System.out.println(myTuple.size());
			myTuple.append(filename);
			for (int i = 0; i < myTuple.size(); i++) {
				System.out.println(DataType.findTypeName(myTuple.getType(i)));
			}
		}
		return myTuple;
	}

}