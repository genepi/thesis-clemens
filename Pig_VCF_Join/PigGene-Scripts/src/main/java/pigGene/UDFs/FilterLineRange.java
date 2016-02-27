package pigGene.UDFs;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 * UDF to filter lines that are not in a given range.
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
public class FilterLineRange extends FilterFunc {
	private static int countLines = 0;

	@Override
	public Boolean exec(final Tuple input) throws IOException {
		if (input == null || input.size() != 2) {
			return false;
		}
		final int start = (Integer) input.get(0);
		final int end = (Integer) input.get(1);
		countLines++;
		return (countLines >= start && countLines <= end);
	}

}