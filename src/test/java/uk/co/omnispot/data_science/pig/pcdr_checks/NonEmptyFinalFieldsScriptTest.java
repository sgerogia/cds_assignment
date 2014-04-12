package uk.co.omnispot.data_science.pig.pcdr_checks;

import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

public class NonEmptyFinalFieldsScriptTest extends BaseCheckScriptTest {

	private static final String ERROR_DATA = "/uk/co/omnispot/data_science/pig/pcdr_checks/end_fields_not_ok.txt";

	private static final String SCRIPT = "./src/main/scripts/pig/pcdr_checks/pcdr_non_empty_final_fields.pig";

	private PigTest test;

	@Test
	public void shouldFindNonEmptyFields() throws Exception {

		test = getTestObject(SCRIPT, ERROR_DATA);
		test.assertOutput(new String[] { "(20110120,875784891,0204,,bar)",
				"(20110120,875784891,0204,foo,)" });
	}

	@Test
	public void allOkForCorrectData() throws Exception {
		test = getTestObject(SCRIPT, ALL_OK);
		test.assertOutput(new String[] {});
	}
}
