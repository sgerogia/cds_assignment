/**
 * 
 */
package uk.co.omnispot.data_science.pig.xml_checks;

import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

import uk.co.omnispot.data_science.pig.BasePigTest;

/**
 * @author Stelios Gerogiannakis <sgerogia@gmail.com>
 */
public class FourFieldsPerRowScriptTest extends BasePigTest {

	private static final String NO_GENDER = "/uk/co/omnispot/data_science/pig/xml_checks/no_gender.xml";

	private static final String NO_ID = "/uk/co/omnispot/data_science/pig/xml_checks/no_id.xml";

	private static final String NO_AGE = "/uk/co/omnispot/data_science/pig/xml_checks/no_age.xml";

	private static final String NO_INCOME = "/uk/co/omnispot/data_science/pig/xml_checks/no_income.xml";

	private static final String ALL_OK = "/uk/co/omnispot/data_science/pig/patients_ok.xml";

	private static final String SCRIPT = "./src/main/scripts/pig/xml_checks/four_fields_per_row.pig";

	private PigTest test;

	@Test
	public void shouldDetectMissingGender() throws Exception {

		test = getAsciiTestObject(SCRIPT, NO_GENDER);
		test.assertOutput(new String[] { "(506013497,75-84,,16000-23999)",
				"(432041392,85+,,&lt;16000)" });
	}

	@Test
	public void shouldDetectMissingId() throws Exception {

		test = getAsciiTestObject(SCRIPT, NO_ID);
		test.assertOutput(new String[] { "(,75-84,F,32000-47999)",
				"(,75-84,M,16000-23999)" });
	}

	@Test
	public void shouldDetectMissingAge() throws Exception {

		test = getAsciiTestObject(SCRIPT, NO_AGE);
		test.assertOutput(new String[] { "(910997967,,F,32000-47999)",
				"(506013497,,M,16000-23999)" });
	}

	@Test
	public void shouldDetectMissingIncome() throws Exception {

		test = getAsciiTestObject(SCRIPT, NO_INCOME);
		test.assertOutput(new String[] { "(506013497,75-84,M,)",
				"(432041392,85+,M,)" });
	}

	@Test
	public void allOkForCorrectData() throws Exception {
		test = getAsciiTestObject(SCRIPT, ALL_OK);
		test.assertOutput(new String[] {});
	}

}
