package pigGene.UDFs;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 * UDF to filter unwanted chromosome information.
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
public class FilterChromosome extends FilterFunc {
	private static ArrayList<String> unwantedChromosomes = new ArrayList<String>(3);

	static {
		unwantedChromosomes.add("M");
		unwantedChromosomes.add("X");
		unwantedChromosomes.add("Y");
	}

	@Override
	public Boolean exec(final Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return false;
		}
		final String chromosome = (String) input.get(0);
		return !unwantedChromosomes.contains(chromosome);
	}

}