package pigGene.UDFs;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 * UDF to compare two filenames and find out which filename comes
 * lexicographically first.
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
public class CompareFilenames extends FilterFunc {

	@Override
	public Boolean exec(final Tuple input) throws IOException {
		if (input == null || input.size() != 2) {
			return false;
		}
		final String a = (String) input.get(0);
		final String b = (String) input.get(1);
		return (a.compareTo(b) < 0);
	}

}