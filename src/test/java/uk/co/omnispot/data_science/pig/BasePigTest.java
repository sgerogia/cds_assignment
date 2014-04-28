/**
 * 
 */
package uk.co.omnispot.data_science.pig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.pigunit.PigTest;
import org.python.google.common.io.Closeables;
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

	protected File createTempFile(String res) throws IOException {

		return createTempFile(res, true);
	}

	protected File createTempFile(String res, boolean isResource)
			throws IOException {

		Reader in = null;
		try {
			if (isResource) {
				in = new InputStreamReader(getClass().getResourceAsStream(res),
						Charsets.US_ASCII);
			} else {
				in = new InputStreamReader(new FileInputStream(res),
						Charsets.US_ASCII);
			}
			String raw = CharStreams.toString(in);
			File tmp = File.createTempFile("PigLoaderTest", ".data");
			tmp.deleteOnExit();
			Files.append(raw, tmp, Charsets.US_ASCII);
			return tmp;
		} finally {
			Closeables.closeQuietly(in);
		}
	}

	protected PigTest getAsciiTestObject(String scriptFile,
			String inputDataResource) throws IOException {

		File tmp = createTempAsciiTextFile(inputDataResource);
		PigTest test = new PigTest(scriptFile, new String[] {
				"INPUT=" + tmp.getAbsolutePath(), "PIGGYBANK=" + PIGGY_BANK,
				"OUTPUT=foo" });
		return test;
	}

	protected PigTest getCsvTestObject(String scriptFile,
			String... inputDataResources) throws IOException {

		List<String> tmpFiles = new LinkedList<String>();
		for (String res : inputDataResources) {
			tmpFiles.add(createTempFile(res, false).getAbsolutePath());
		}
		PigTest test = new PigTest(scriptFile, new String[] {
				"INPUT=" + StringUtils.join(tmpFiles, ','),
				"PIGGYBANK=" + PIGGY_BANK, "OUTPUT=foo" });
		return test;
	}

}
