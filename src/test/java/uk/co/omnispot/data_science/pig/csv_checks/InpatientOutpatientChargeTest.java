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

	private static final String COUNT_RESULT = "/uk/co/omnispot/data_science/pig/csv_checks/inpatient_outpatient_charge_counts.txt";

	private static final String UNIQUE_RESULT = "/uk/co/omnispot/data_science/pig/csv_checks/inpatient_outpatient_charge_unique_keys.txt";

	private static final String COUNT_SCRIPT = "./src/main/scripts/pig/csv_checks/inpatient_outpatient_charge/count.pig";

	private static final String UNIQUE_SCRIPT = "./src/main/scripts/pig/csv_checks/inpatient_outpatient_charge/unique_operation_provider_combo.pig";

	private static final String INPATIENT = "Medicare_Provider_Charge_Inpatient_DRG100_FY2011.csv";

	private static final String OUTPATIENT = "Medicare_Provider_Charge_Outpatient_APC30_CY2011_v2.csv";

	private PigTest testObject;

	@Test
	public void inPatientOutPatientCountsAsReturnedFromStreaming()
			throws Exception {

		testObject = getCsvTestObject(COUNT_SCRIPT, outpatientCharge
				+ OUTPATIENT, inpatientCharge + INPATIENT);
		testObject.assertOutput(loadStreamingResultsAsPigResults(COUNT_RESULT));
	}

	@Test
	public void ensureUniqueOperationAndProviderIdGroups() throws Exception {

		testObject = getCsvTestObject(UNIQUE_SCRIPT, outpatientCharge
				+ OUTPATIENT, inpatientCharge + INPATIENT);
		testObject
				.assertOutput(loadStreamingResultsAsPigResults(UNIQUE_RESULT));
	}

}
