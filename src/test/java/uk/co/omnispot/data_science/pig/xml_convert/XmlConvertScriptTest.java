/**
 * 
 */
package uk.co.omnispot.data_science.pig.xml_convert;

import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

import uk.co.omnispot.data_science.pig.BasePigTest;

/**
 * 
 * @author Stelios Gerogiannakis <sgerogia@gmail.com>
 */
public class XmlConvertScriptTest extends BasePigTest {

	private static final String ALL_OK = "/uk/co/omnispot/data_science/pig/patients_ok.xml";

	private static final String SCRIPT = "./src/main/scripts/pig/xml_convert/convert_to_tab_delimited.pig";

	private PigTest test;

	@Test
	public void shouldConvertFile() throws Exception {

		test = getTestObject(SCRIPT, ALL_OK);
		test.assertOutput(new String[] { "(910997967,&lt;75,F,32000-47999)",
				"(506013497,75-84,M,16000-23999)",
				"(432041392,85+,M,&lt;16000)", "(432041392,85+,M,35000+)" });
	}

}
