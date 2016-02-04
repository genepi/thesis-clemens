package utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 14.08.15.
 */
public class BaseSequenceContentWritable implements Writable {
    private static final char BASE_A = 'A';
    private static final char BASE_C = 'C';
    private static final char BASE_G = 'G';
    private static final char BASE_T = 'T';

    private double percentageBaseA;
    private double percentageBaseC;
    private double percentageBaseG;
    private double percentageBaseT;

    public BaseSequenceContentWritable(double percentageBaseA, double percentageBaseC, double percentageBaseG, double percentageBaseT) {
        this.percentageBaseA = percentageBaseA;
        this.percentageBaseC = percentageBaseC;
        this.percentageBaseG = percentageBaseG;
        this.percentageBaseT = percentageBaseT;
    }

    public BaseSequenceContentWritable(Map<Character, Double> percentages) {
        this.percentageBaseA = percentages.get(BASE_A);
        this.percentageBaseC = percentages.get(BASE_C);
        this.percentageBaseG = percentages.get(BASE_G);
        this.percentageBaseT = percentages.get(BASE_T);
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(percentageBaseA);
        out.writeDouble(percentageBaseC);
        out.writeDouble(percentageBaseG);
        out.writeDouble(percentageBaseT);
    }

    public void readFields(DataInput in) throws IOException {
        percentageBaseA = in.readDouble();
        percentageBaseC = in.readDouble();
        percentageBaseG = in.readDouble();
        percentageBaseT = in.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BaseSequenceContentWritable that = (BaseSequenceContentWritable) o;

        if (Double.compare(that.percentageBaseA, percentageBaseA) != 0) return false;
        if (Double.compare(that.percentageBaseC, percentageBaseC) != 0) return false;
        if (Double.compare(that.percentageBaseG, percentageBaseG) != 0) return false;
        return Double.compare(that.percentageBaseT, percentageBaseT) == 0;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(percentageBaseA);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(percentageBaseC);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(percentageBaseG);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(percentageBaseT);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        final String tabulator = "\t";
        return percentageBaseA + tabulator + percentageBaseC + tabulator + percentageBaseG + tabulator + percentageBaseT;
    }

}