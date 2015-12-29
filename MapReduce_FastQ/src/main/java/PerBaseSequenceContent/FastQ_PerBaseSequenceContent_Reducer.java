package PerBaseSequenceContent;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import utils.BaseSequenceContentWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 06.08.15.
 */
public class FastQ_PerBaseSequenceContent_Reducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, BaseSequenceContentWritable> {
    private static final char BASE_A = 'A';
    private static final char BASE_C = 'C';
    private static final char BASE_G = 'G';
    private static final char BASE_T = 'T';

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key, new BaseSequenceContentWritable(getPercentageOfBaseOccurrences(values)));
    }

    private Map<Character, Double> getPercentageOfBaseOccurrences(Iterable<Text> values) {
        final Map<Character, Double> result = new HashMap<Character, Double>(4);
        int noOfBaseA = 0;
        int noOfBaseC = 0;
        int noOfBaseG = 0;
        int noOfBaseT = 0;

        for (Text element : values) {
            char base = element.toString().charAt(0);
            switch (base) {
                case BASE_A:
                    noOfBaseA++;
                    break;
                case BASE_C:
                    noOfBaseC++;
                    break;
                case BASE_G:
                    noOfBaseG++;
                    break;
                case BASE_T:
                    noOfBaseT++;
                    break;
                default:
                    break;
            }
        }

        final int totalNoOfBases = noOfBaseA + noOfBaseC + noOfBaseG + noOfBaseT;
        result.put(BASE_A, calculatePercentageValue(noOfBaseA, totalNoOfBases));
        result.put(BASE_C, calculatePercentageValue(noOfBaseC, totalNoOfBases));
        result.put(BASE_G, calculatePercentageValue(noOfBaseG, totalNoOfBases));
        result.put(BASE_T, calculatePercentageValue(noOfBaseT, totalNoOfBases));
        return result;
    }

    private double calculatePercentageValue(int baseOccurrence, int totalNoOfBases) {
        return (double) (baseOccurrence * 100) / (double) totalNoOfBases;
    }

}