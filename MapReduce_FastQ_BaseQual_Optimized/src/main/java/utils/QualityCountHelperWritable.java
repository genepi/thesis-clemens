package utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 04.02.16.
 */
public class QualityCountHelperWritable implements Writable {
    private int sumOfQualityValues = 0;
    private int numberOfQualityValues = 0;

    public QualityCountHelperWritable() {
    }

    public QualityCountHelperWritable(int sumOfQualityValues, int numberOfQualityValues) {
        this.sumOfQualityValues = sumOfQualityValues;
        this.numberOfQualityValues = numberOfQualityValues;
    }

    public QualityCountHelperWritable(int sumOfQualityValues) {
        this(sumOfQualityValues, 1);
    }

    public int getSumOfQualityValues() {
        return sumOfQualityValues;
    }

    public int getNumberOfQualityValues() {
        return numberOfQualityValues;
    }

    public void addQualityValueToSum(int value) {
        this.sumOfQualityValues += value;
    }

    public void incrementNumberOfQualityValues() {
        this.numberOfQualityValues++;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(sumOfQualityValues);
        out.writeInt(numberOfQualityValues);
    }

    public void readFields(DataInput in) throws IOException {
        this.sumOfQualityValues = in.readInt();
        this.numberOfQualityValues = in.readInt();
    }

}