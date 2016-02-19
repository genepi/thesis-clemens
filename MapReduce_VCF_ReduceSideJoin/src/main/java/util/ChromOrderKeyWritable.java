package util;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 18.02.16.
 */
public class ChromOrderKeyWritable implements WritableComparable<ChromOrderKeyWritable> {
    //natural key part
    private int chromosome;
    private int position;

    //secondary key part
    private int orderVal;

    public ChromOrderKeyWritable() {
    }

    public ChromOrderKeyWritable(int chromosome, int position, int orderVal) {
        this.chromosome = chromosome;
        this.position = position;
        this.orderVal = orderVal;
    }

    public int compareTo(ChromOrderKeyWritable that) {
        int result = (this.chromosome < that.chromosome ? -1 : (this.chromosome == that.chromosome ? 0 : 1));
        if (0 == result) {
            result = (this.position < that.position ? -1 : (this.position == that.position ? 0 : 1));
        }
        if (0 == result) {
            result = (this.orderVal < that.orderVal ? -1 : (this.orderVal == that.orderVal ? 0 : 1));
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(chromosome);
        out.writeInt(position);
        out.writeInt(orderVal);
    }

    public void readFields(DataInput in) throws IOException {
        chromosome = in.readInt();
        position = in.readInt();
        orderVal = in.readInt();
    }

    public int getChromosome() {
        return chromosome;
    }

    public int getPosition() {
        return position;
    }

    public int getOrderVal() {
        return orderVal;
    }

    @Override
    public String toString() {
        return "chrom: " + this.chromosome + ", pos: " + position + ", orderVal: " + this.orderVal;
    }

}