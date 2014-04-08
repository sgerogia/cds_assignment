/**
 * 
 */
package uk.co.omnispot.data_science.input;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * Returns file offset as key and record as a tab-delimited text. Maximum line
 * length can be configured by <code>mapred.linerecordreader.maxlength</code>.
 * 
 * @see <a
 *      href="ASCII delimited text">http://en.wikipedia.org/wiki/Delimiter#ASCII_delimited_text</a>
 * @author Stelios Gerogiannakis <sgerogia@gmail.com>
 */
public class AsciiDelimitedTextRecordReader extends
		org.apache.hadoop.mapreduce.RecordReader<LongWritable, Text> {

	private static final Log LOG = LogFactory
			.getLog(AsciiDelimitedTextRecordReader.class.getName());

	private static final byte[] RECORD_DELIMITER = new byte[] { 30 };

	public static final String FIELD_DELIMITER_STRING = "\u001f";

	public static final String RECORD_DELIMITER_STRING = "\u001e";

	private long start;
	private long end;
	private long pos;

	private int maxLineLength;

	private LineReader reader;

	private LongWritable key;
	private Text value;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);

		FileSplit split = (FileSplit) inputSplit;
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(conf);

		LOG.info("Opening " + AsciiDelimitedTextRecordReader.class.getName()
				+ " on split for " + file.getName());

		reader = new LineReader(fs.open(split.getPath()), 128, RECORD_DELIMITER);
		this.pos = start;

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
		while (pos < end) {
			newSize = reader.readLine(value, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));
			if (newSize == 0) {
				break;
			}
			pos += newSize;
			if (newSize < maxLineLength) {
				break;
			}

			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos "
					+ (pos - newSize));
		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {

		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {

		return value;
	}

	@Override
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public synchronized void close() throws IOException {
		if (reader != null) {
			reader.close();
		}
	}
}
