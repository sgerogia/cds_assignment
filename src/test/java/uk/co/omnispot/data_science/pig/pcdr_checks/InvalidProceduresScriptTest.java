package uk.co.omnispot.data_science.pig.pcdr_checks;

import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

public class InvalidProceduresScriptTest extends BaseCheckScriptTest {

	private static final String ERROR_DATA = "/uk/co/omnispot/data_science/pig/pcdr_checks/procedures_not_ok.txt";

	private static final String SCRIPT = "./src/main/scripts/pig/pcdr_checks/pcdr_invalid_procedures.pig";

	private PigTest test;

	@Test
	public void shouldFindWrongPatientId() throws Exception {

		test = getAsciiTestObject(SCRIPT, ERROR_DATA);
		test.assertOutput(new String[] { "(20110120,875784891,02a4,,)" });
	}

	@Test
	public void allOkForCorrectData() throws Exception {
		test = getAsciiTestObject(SCRIPT, ALL_OK);
		test.assertOutput(new String[] {});
	}
}
