/**
 * 
 */
package uk.co.omnispot.data_science.pig;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import uk.co.omnispot.data_science.input.AsciiDelimitedTextInputFormat;
import uk.co.omnispot.data_science.input.AsciiDelimitedTextRecordReader;

/**
 * Reader for the {@link AsciiDelimitedTextInputFormat}.
 * 
 * @author Stelios Gerogiannakis <sgerogia@gmail.com>
 */
public class AsciiDelimitedTextLoader extends LoadFunc {

	private AsciiDelimitedTextRecordReader reader;

	private final TupleFactory tupleFactory;

	public AsciiDelimitedTextLoader() {

		tupleFactory = TupleFactory.getInstance();
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.LoadFunc#getNext()
	 */
	@Override
	public Tuple getNext() throws IOException {

		Tuple tuple = null;

		try {
			boolean hasNext = reader.nextKeyValue();
			if (!hasNext) {
				return null;
			}
			Text value = reader.getCurrentValue();

			if (value != null) {
				String parts[] = value.toString().split(
						AsciiDelimitedTextRecordReader.FIELD_DELIMITER_STRING);

				tuple = tupleFactory.newTuple(Arrays.asList(parts));
			}
		} catch (InterruptedException e) {
			// add more information to the runtime exception condition.
			int errCode = 666;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode,
					PigException.REMOTE_ENVIRONMENT, e);
		}

		return tuple;
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {

		FileInputFormat.setInputPaths(job, location);
	}

	@Override
	public InputFormat getInputFormat() throws IOException {

		return new AsciiDelimitedTextInputFormat();
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {

		this.reader = (AsciiDelimitedTextRecordReader) reader;
	}

}
