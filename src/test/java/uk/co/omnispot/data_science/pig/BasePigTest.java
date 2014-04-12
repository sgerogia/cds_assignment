/**
 * 
 */
package uk.co.omnispot.data_science.pig;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.pig.pigunit.PigTest;
import org.python.google.common.io.Files;

import uk.co.omnispot.data_science.input.AsciiDelimitedTextRecordReader;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

/**
 * Base class for Pig tests.
 * 
 * @author Stelios Gerogiannakis <sgerogia@gmail.com>
 */
public abstract class BasePigTest {

	// FIXME: This is NOT the right way!
	private static final String PIGGY_BANK = "/usr/lib/pig/piggybank.jar";

	protected File createTempAsciiTextFile(String res) throws IOException {

		String raw = CharStreams.toString(new InputStreamReader(getClass()
				.getResourceAsStream(res), Charsets.US_ASCII));
		String converted = raw
				.replace(".",
						AsciiDelimitedTextRecordReader.FIELD_DELIMITER_STRING)
				.replace("|",
						AsciiDelimitedTextRecordReader.RECORD_DELIMITER_STRING)
				.replace("\n", "");
		File tmp = File.createTempFile("AsciiDelimitedTextLoaderTest", ".data");
		tmp.deleteOnExit();
		Files.append(converted, tmp, Charsets.US_ASCII);
		return tmp;
	}

	protected PigTest getTestObject(String scriptFile, String inputDataResource)
			throws IOException {

		File tmp = createTempAsciiTextFile(inputDataResource);
		PigTest test = new PigTest(scriptFile, new String[] {
				"INPUT=" + tmp.getAbsolutePath(), "PIGGYBANK=" + PIGGY_BANK,
				"OUTPUT=foo" });
		return test;
	}

}
