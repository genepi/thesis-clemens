package pigGene.UDFs;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 * UDF to filter all lines containing an rs-Number.
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
public class RemoveRsNumber extends FilterFunc {
	private static String rsNumber = "rs";

	@Override
	public Boolean exec(final Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return false;
		}
		String rs = null;
		try {
			rs = ((String) input.get(0)).substring(0, 2);
		} catch (final IndexOutOfBoundsException e) {
			return true;
		}
		return !rsNumber.equals(rs);
	}

}