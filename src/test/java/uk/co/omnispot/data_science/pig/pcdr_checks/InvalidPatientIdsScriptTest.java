package uk.co.omnispot.data_science.pig.pcdr_checks;

import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

public class InvalidPatientIdsScriptTest extends BaseCheckScriptTest {

	private static final String ERROR_DATA = "/uk/co/omnispot/data_science/pig/pcdr_checks/patient_ids_not_ok.txt";

	private static final String SCRIPT = "./src/main/scripts/pig/pcdr_checks/pcdr_invalid_patient_ids.pig";

	private PigTest test;

	@Test
	public void shouldFindWrongPatientId() throws Exception {

		test = getAsciiTestObject(SCRIPT, ERROR_DATA);
		test.assertOutput(new String[] { "(20110120,87578489a1,0204,,)" });
	}

	@Test
	public void allOkForCorrectData() throws Exception {
		test = getAsciiTestObject(SCRIPT, ALL_OK);
		test.assertOutput(new String[] {});
	}
}
