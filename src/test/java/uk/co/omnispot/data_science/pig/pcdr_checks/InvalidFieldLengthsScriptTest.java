package uk.co.omnispot.data_science.pig.pcdr_checks;

import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

public class InvalidFieldLengthsScriptTest extends BaseCheckScriptTest {

	private static final String ERROR_DATA = "/uk/co/omnispot/data_science/pig/pcdr_checks/lengths_not_ok.txt";

	private static final String SCRIPT = "./src/main/scripts/pig/pcdr_checks/pcdr_invalid_field_lengths.pig";

	private PigTest test;

	@Test
	public void shouldFindWrongLengths() throws Exception {

		test = getTestObject(SCRIPT, ERROR_DATA);
		test.assertOutput(new String[] { "(20110114,198158662,604,,)",
				"(2011012012,875784891,0204,,)", "(20110120,75784891,0204,,)" });
	}

	@Test
	public void allOkForCorrectData() throws Exception {
		test = getTestObject(SCRIPT, ALL_OK);
		test.assertOutput(new String[] {});
	}
}
