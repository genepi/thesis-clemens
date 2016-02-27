package pigGene.UDFs;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * UDF to extract the quality.
 * 
 * @author: Clemens Banas
 * @date: April 2013
 */
public class ExtractQuality extends EvalFunc<String> {

	@Override
	public String exec(final Tuple input) throws IOException {
		final String sample = (String) input.get(0);
		return sample.substring((sample.lastIndexOf(":") + 1), sample.length());
	}

}