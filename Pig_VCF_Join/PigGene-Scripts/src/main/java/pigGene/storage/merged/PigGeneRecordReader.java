package pigGene.storage.merged;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class PigGeneRecordReader extends RecordReader<LongWritable, Text> {
	private static final int leadingInfoFields = 9;
	private static final int infoColOffset = 7;
	private static final String delimiter = "\t";
	private final LinkedList<String> readLines = new LinkedList<String>();
	private CompressionCodecFactory compressionCodecs = null;
	private final byte[] recordDelimiterBytes;
	private LongWritable key = null;
	private Text value = null;
	private LineReader in;

	private long keyCounter = 0;
	private long start;
	private long pos;
	private long end;
	private int maxLineLength;

	private final Text textLine = new Text();
	private final String referenceValue = "0/0";
	private final String indelValue = "INDEL";

	public PigGeneRecordReader(final byte[] recordDelimiter) {
		recordDelimiterBytes = recordDelimiter;
	}

	public PigGeneRecordReader() {
		this(null);
	}

	@Override
	public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException, InterruptedException {
		final FileSplit split = (FileSplit) genericSplit;
		final Configuration job = context.getConfiguration();

		// using the maximum length specified in
		// the configuration of the LineRecordReader...
		maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);

		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek the start of the split
		final FileSystem fs = file.getFileSystem(job);
		final FSDataInputStream fileIn = fs.open(split.getPath());
		boolean skipFirstLine = false;

		if (codec != null) {
			if (null == recordDelimiterBytes) {
				in = new LineReader(codec.createInputStream(fileIn), job);
			} else {
				in = new LineReader(codec.createInputStream(fileIn), job, recordDelimiterBytes);
			}
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
			if (null == recordDelimiterBytes) {
				in = new LineReader(fileIn, job);
			} else {
				in = new LineReader(fileIn, job, recordDelimiterBytes);
			}
		}
		if (skipFirstLine) { // skip first line and re-establish "start".
			start += in.readLine(new Text(), 0, (int) Math.min(Integer.MAX_VALUE, end - start));
		}
		pos = start;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// if LinkedList contains elements - consume them
		if (!readLines.isEmpty()) {
			return true;
		}

		if (value == null) {
			value = new Text();
		}
		if (key == null) {
			key = new LongWritable();
		}
		key.set(keyCounter++);

		int newSize = 0;
		boolean headerLine = false;
		boolean addedLine = false;
		while (pos < end || headerLine) {
			newSize = in.readLine(value, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
			if (newSize == 0) { // every line was read
				break;
			}
			pos += newSize;
			if (newSize < maxLineLength) { // line length okay?
				if (value.charAt(0) == '#') { // ignore! -> header line
					headerLine = true;
				} else { // something new to read: split the new line and put it
							// into queue if split is possible (not the whole
							// line filtered) otherwise continue reading
					addedLine = splitLine(key, value);
					if (addedLine) {
						return true;
					}
				}
			}
		}

		// only return FALSE if LinkedList is empty and nothing more to read...
		key = null;
		value = null;
		return false;
	}

	private boolean splitLine(final LongWritable key, final Text value) {
		boolean addedLine = false;
		final String inputLine = value.toString();
		final String[] tmpSplits = inputLine.split(delimiter);

		if (!infoMatchesINDEL(tmpSplits[infoColOffset])) {
			// create leading info
			final StringBuilder leadingInfoBuffer = new StringBuilder();
			for (int i = 0; i < leadingInfoFields; i++) {
				leadingInfoBuffer.append(tmpSplits[i]).append(delimiter);
			}

			// append person and id column
			int idCounter = 0;
			final String leadingInfo = leadingInfoBuffer.toString();
			final int noOfTestPersons = tmpSplits.length - leadingInfoFields;
			String genotype;
			for (int i = 0; i < noOfTestPersons; i++) {
				final StringBuilder text = new StringBuilder();
				genotype = tmpSplits[leadingInfoFields + idCounter];

				// optional: ignore lines that equal the reference
				// if (!genotypeMatchesReference(genotype)) {
				text.append(leadingInfo); // leading fields
				text.append(genotype).append(delimiter); // testPerson
				text.append(Integer.toString(idCounter)); // testPersonId
				readLines.add(text.toString());
				addedLine = true;
				// }
				idCounter++;
			}
		}
		return addedLine;
	}

	/**
	 * The infoMatchesINDEL method checks if the given String (infoSub
	 * parameter) matches "INDEL".
	 * 
	 * @param infoSub
	 * @return true if given String is not null, it's length is larger or equal
	 *         than 5 and the first 5 characters match the "INDEL" keyword.
	 *         false otherwise.
	 */
	private boolean infoMatchesINDEL(final String infoSub) {
		if (infoSub != null && infoSub.length() >= 5 && indelValue.equals(infoSub.substring(0, 5))) {
			return true;
		}
		return false;
	}

	/**
	 * The genotypeMatchesReference method checks if the given String (genotype
	 * parameter) matches the reference value (which is "0/0").
	 * 
	 * @param genotype
	 * @return true if given String is not null, it's length is larger or equal
	 *         than 5 and the first 3 characters match the reference value
	 *         ("0/0"). false otherwise.
	 */
	@SuppressWarnings("unused")
	private boolean genotypeMatchesReference(final String genotype) {
		if (genotype != null && genotype.length() >= 3 && referenceValue.equals(genotype.substring(0, 3))) {
			return true;
		}
		return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		textLine.set(readLines.poll());
		return textLine;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

}