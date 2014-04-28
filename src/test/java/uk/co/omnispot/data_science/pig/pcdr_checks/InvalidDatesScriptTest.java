/**
 * 
 */
package uk.co.omnispot.data_science.pig.pcdr_checks;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

/**
 * @author Stelios Gerogiannakis <sgerogia@gmail.com>
 * 
 */
public class InvalidDatesScriptTest extends BaseCheckScriptTest {

	private static final String ERROR_DATA1 = "/uk/co/omnispot/data_science/pig/pcdr_checks/dates_not_ok1.txt";

	private static final String ERROR_DATA2 = "/uk/co/omnispot/data_science/pig/pcdr_checks/dates_not_ok2.txt";

	private static final String SCRIPT = "./src/main/scripts/pig/pcdr_checks/pcdr_invalid_dates.pig";

	private PigTest test;

	@Test(expected = FrontendException.class)
	public void shouldFailForIncorrectFormat() throws Exception {

		test = getAsciiTestObject(SCRIPT, ERROR_DATA1);
		test.assertOutput(new String[] {});
	}

	@Test(expected = FrontendException.class)
	public void shouldFailForIncorrectNumber() throws Exception {

		test = getAsciiTestObject(SCRIPT, ERROR_DATA2);
		test.assertOutput(new String[] {});
	}

	@Test
	public void allOkForCorrectData() throws Exception {
		test = getAsciiTestObject(SCRIPT, ALL_OK);
		test.assertOutput(new String[] {});
	}
}
