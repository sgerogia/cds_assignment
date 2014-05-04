/**
 * 
 */
package uk.co.omnispot.data_science.pig.answers.one_b;

import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

import uk.co.omnispot.data_science.pig.csv_checks.BaseCsvCheckTest;

/**
 * Unit test for the 1b solution script.
 * 
 * @author Stelios Gerogiannakis <sgerogia@gmail.com>
 */
public class AnswerOneBTest extends BaseCsvCheckTest {

	private static final String COUNT_RESULT = "/uk/co/omnispot/data_science/pig/answers/one_b/results.txt";

	private static final String COUNT_SCRIPT = "./src/main/scripts/pig/questions/one_b/count.pig";

	private static final String DATA = "./src/test//resources/uk/co/omnispot/data_science/pig/answers/one_b/data.csv";

	private PigTest testObject;

	@Test
	public void topChargingProvidersAsExpectedInTestScenario() throws Exception {

		testObject = getCsvTestObject(COUNT_SCRIPT, DATA);
		testObject.assertOutput(loadStreamingResultsAsPigResults(COUNT_RESULT));
	}

}
