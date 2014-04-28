/**
 * 
 */
package uk.co.omnispot.data_science.pig.csv_checks;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;

import uk.co.omnispot.data_science.pig.BasePigTest;

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;

/**
 * Base class for all CSV check tests. Requires the following system properties
 * which point to the corresponding folder locations: INPATIENT_CHARGE,
 * OUTPATIENT_CHARGE, INPATIENT_CHARGE_SUMMARY, OUTPATIENT_CHARGE_SUMMARY.
 * 
 * @author Stelios Gerogiannakis <sgerogia@gmail.com>
 */
public class BaseCsvCheckTest extends BasePigTest {

	private static final String BASE_DATA_FOLDER = "/media/sf_dsc_data/";

	protected String inpatientCharge;
	protected String outpatientCharge;
	protected String inpatientChargeSummary;
	protected String outpatientChargeSummary;

	protected String[] loadStreamingResultsAsPigResults(String res)
			throws IOException {

		List<String> lines = new ArrayList<String>();
		String line = null;
		String[] parts = null;
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(getClass()
					.getResourceAsStream(res), Charsets.UTF_8));
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				parts = line.split(" ");
				line = '(' + parts[1] + ',' + parts[0] + ')';
				lines.add(line);
			}
		} finally {
			Closeables.closeQuietly(reader);
		}
		return lines.toArray(new String[lines.size()]);
	}

	@Before
	public void loadSystemProperties() {

		inpatientCharge = System.getProperty("INPATIENT_CHARGE",
				BASE_DATA_FOLDER + "INPATIENT-CHARGE/");
		inpatientChargeSummary = System.getProperty("INPATIENT_CHARGE_SUMMARY",
				BASE_DATA_FOLDER + "INPATIENT-CHARGE-SUMMARY/");
		outpatientCharge = System.getProperty("OUTPATIENT_CHARGE",
				BASE_DATA_FOLDER + "OUTPATIENT-CHARGE/");
		outpatientChargeSummary = System.getProperty(
				"OUTPATIENT_CHARGE_SUMMARY", BASE_DATA_FOLDER
						+ "OUTPATIENT-CHARGE-SUMMARY/");
	}
}
