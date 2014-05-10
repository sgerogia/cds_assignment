/**
 * 
 */
package uk.co.omnispot.data_science.pig.answers.one_c;

import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

import uk.co.omnispot.data_science.pig.csv_checks.BaseCsvCheckTest;

/**
 * Unit test for the 1c solution script.
 * 
 * @author Stelios Gerogiannakis <sgerogia@gmail.com>
 */
public class AnswerOneCTest extends BaseCsvCheckTest {

	private static final String COUNT_RESULT = "/uk/co/omnispot/data_science/pig/answers/one_c/results.txt";

	private static final String COUNT_SCRIPT = "./src/main/scripts/pig/questions/one_c/count.pig";

	private static final String DATA = "./src/test//resources/uk/co/omnispot/data_science/pig/answers/one_c/data.csv";

	private PigTest testObject;

	@Test
	public void topChargingProvidersAsExpectedInTestScenario() throws Exception {

		testObject = getCsvTestObject(COUNT_SCRIPT, DATA);
		testObject.assertOutput(loadStreamingResultsAsPigResults(COUNT_RESULT));
	}

}
