/**
 * 
 */
package uk.co.omnispot.data_science.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Input format to handle ASCII delimited text.
 * 
 * @see <a
 *      href="ASCII delimited text">http://en.wikipedia.org/wiki/Delimiter#ASCII_delimited_text</a>
 * @author Stelios Gerogiannakis <sgerogia@gmail.com>
 */
public class AsciiDelimitedTextInputFormat extends
		FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext ctx) throws IOException {

		ctx.setStatus(split.toString());
		return new AsciiDelimitedTextRecordReader();
	}

}
