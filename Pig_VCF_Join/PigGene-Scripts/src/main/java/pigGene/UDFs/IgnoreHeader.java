package pigGene.UDFs;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 * UDF to remove unwanted header information in the pig relation to speed up
 * execution.
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
public class IgnoreHeader extends FilterFunc {

	@Override
	public Boolean exec(final Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return false;
		}
		final String value = (String) input.get(0);
		final String headerSymbol = "#";
		return !(value.substring(0, 1).equals(headerSymbol));
	}

}