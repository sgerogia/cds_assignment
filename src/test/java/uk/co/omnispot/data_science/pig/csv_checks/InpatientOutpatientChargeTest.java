/**
 * 
 */
package uk.co.omnispot.data_science.pig.csv_checks;

import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

/**
 * @author Stelios Gerogiannakis <sgerogia@gmail.com>
 */
public class InpatientOutpatientChargeTest extends BaseCsvCheckTest {

	private static final String RESULT = "/uk/co/omnispot/data_science/pig/csv_checks/inpatient_outpatient_charge_counts.txt";

	private static final String SCRIPT = "./src/main/scripts/pig/csv_checks/inpatient_outpatient_charge/count.pig";

	private static final String INPATIENT = "Medicare_Provider_Charge_Inpatient_DRG100_FY2011.csv";

	private static final String OUTPATIENT = "Medicare_Provider_Charge_Outpatient_APC30_CY2011_v2.csv";

	private PigTest testObject;

	@Test
	public void shouldReturnExpectedResult() throws Exception {

		testObject = getCsvTestObject(SCRIPT, outpatientCharge + OUTPATIENT,
				inpatientCharge + INPATIENT);
		testObject.assertOutput(loadStreamingResultsAsPigResults(RESULT));
	}
}
