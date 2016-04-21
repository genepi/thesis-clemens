package pigGene.UDFs;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * UDF to extract header information in the pig relation.
 * 
 * @author: Clemens Banas
 */
public class ExtractHeader extends FilterFunc {

	@Override
	public Boolean exec(final Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return false;
		}
		final String value = (String) input.get(0);
		final String headerSymbol = "#";
		return (value.substring(0, 1).equals(headerSymbol));
	}

}