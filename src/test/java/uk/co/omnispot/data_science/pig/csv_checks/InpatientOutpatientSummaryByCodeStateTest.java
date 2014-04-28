/**
 * 
 */
package uk.co.omnispot.data_science.pig.csv_checks;

import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

/**
 * @author Stelios Gerogiannakis <sgerogia@gmail.com>
 * 
 */
public class InpatientOutpatientSummaryByCodeStateTest extends BaseCsvCheckTest {

	private static final String RESULT = "/uk/co/omnispot/data_science/pig/csv_checks/inpatient_outpatient_summary_by_code_state_counts.txt";

	private static final String SCRIPT = "./src/main/scripts/pig/csv_checks/inpatient_outpatient_summary_by_code_state/count.pig";

	private static final String INPATIENT = "Medicare_Charge_Inpatient_DRG100_DRG_Summary_by_DRGState_FY2011.csv";

	private static final String OUTPATIENT = "Medicare_Charge_Outpatient_APC30_Summary_by_APCState_CY2011_v2.csv";

	private PigTest testObject;

	@Test
	public void shouldReturnExpectedResult() throws Exception {

		testObject = getCsvTestObject(SCRIPT, inpatientChargeSummary
				+ INPATIENT, outpatientChargeSummary + OUTPATIENT);
		testObject.assertOutput(loadStreamingResultsAsPigResults(RESULT));
	}

}
