package pigGene.storage.reference;

import java.io.IOException;

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

public class PigGeneRecordReaderReferenceFile extends RecordReader<LongWritable, Text> {
	private CompressionCodecFactory compressionCodecs = null;
	private final byte[] recordDelimiterBytes;
	private LongWritable key = null;
	private Text value = null;
	private LineReader in;

	private long start;
	private long pos;
	private long end;
	private Path file;
	private int maxLineLength;

	public PigGeneRecordReaderReferenceFile(final byte[] recordDelimiter) {
		recordDelimiterBytes = recordDelimiter;
	}

	public PigGeneRecordReaderReferenceFile() {
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
		file = split.getPath();
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
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}
		int newSize = 0;
		if (pos < end) {
			boolean skipped;
			do {
				newSize = in.readLine(value, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
				skipped = skipLine(value);
				pos += newSize;
			} while (skipped && pos < end);

			if (!skipped && newSize != 0 && newSize < maxLineLength) {
				return true;
			}
		}

		key = null;
		value = null;
		return false;
	}

	/**
	 * This method returns true if the given Text value, representing an input
	 * line, should be skipped.
	 * 
	 * @param value
	 * @return true if the given text starts with the # symbol that identifies a
	 *         header line.
	 */
	private boolean skipLine(final Text value) {
		if (value.charAt(0) == '#') {
			return true;
		}
		return false;
	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
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