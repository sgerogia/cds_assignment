package uk.co.omnispot.data_science.pig;

import java.io.File;

import org.apache.pig.pigunit.PigTest;
import org.junit.Before;
import org.junit.Test;

public class AsciiDelimitedTextLoaderTest extends BasePigTest {

	private final static String ASCII_RES = "/uk/co/omnispot/data_science/pig/ascii_delimited.txt";

	private File tmp;

	@Before
	public void setUp() throws Exception {

		tmp = createTempAsciiTextFile(ASCII_RES);
	}

	@Test
	public void loaderShouldParseAsciiDelimitedText() throws Exception {

		String[] script = {
				"lines   = load '"
						+ tmp.getAbsolutePath()
						+ "' using uk.co.omnispot.data_science.pig.AsciiDelimitedTextLoader()"
						+ " as (date: chararray, patient: chararray, operation: chararray, field1: chararray, field2: chararray);",
				"store lines into 'parsed_lines';", };
		String[] expectedOutput = { "(20110120,071159871,0096,123,)",
				"(20110114,198158662,0604,,456)",
				"(20110120,875784891,0204,123,456)" };
		PigTest test = new PigTest(script);
		test.assertOutput(expectedOutput);
	}

	@Test
	public void loaderShouldAllowGroupingAndOtherActions() throws Exception {

		String[] script = {
				"lines   = load '"
						+ tmp.getAbsolutePath()
						+ "' using uk.co.omnispot.data_science.pig.AsciiDelimitedTextLoader()"
						+ " as (date: chararray, patient: chararray, operation: chararray, field1: chararray, field2: chararray);",
				"lines_with_date = foreach lines generate ToDate(date, 'yyyyMMdd') as date, (long)patient, operation, field1, field2;",
				"grouped_lines = group lines_with_date by date;",
				"store grouped_lines into 'parsed_lines';", };
		// assumes UTC timezone of machine
		String[] expectedOutput = {
				"(2011-01-14T00:00:00.000Z,{(2011-01-14T00:00:00.000Z,198158662,0604,,456)})",
				"(2011-01-20T00:00:00.000Z,{(2011-01-20T00:00:00.000Z,71159871,0096,123,),(2011-01-20T00:00:00.000Z,875784891,0204,123,456)})" };
		PigTest test = new PigTest(script);
		test.assertOutput(expectedOutput);
	}

}
