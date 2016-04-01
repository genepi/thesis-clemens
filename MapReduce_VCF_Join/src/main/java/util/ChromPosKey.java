package util;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 25.03.16.
 */
public class ChromPosKey implements WritableComparable<ChromPosKey> {

    //natural key part
    private int chromosome;

    //secondary sort operates on this values
    private int position;
    private int orderValue;

    public ChromPosKey() {
    }

    public ChromPosKey(int chromosome, int position, int orderValue) {
        this.chromosome = chromosome;
        this.position = position;
        this.orderValue = orderValue;
    }

    public int getChromosome() {
        return chromosome;
    }

    public void setChromosome(int chromosome) {
        this.chromosome = chromosome;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getOrderValue() {
        return orderValue;
    }

    public void setOrderValue(int orderValue) {
        this.orderValue = orderValue;
    }

    public int compareTo(ChromPosKey that) {
        int result = (this.chromosome < that.chromosome ? -1 : (this.chromosome == that.chromosome ? 0 : 1));
        if (0 == result) {
            result = (this.position < that.position ? -1 : (this.position == that.position ? 0 : 1));
        }
        if (0 == result) {
            result = (this.orderValue < that.orderValue ? -1 : (this.orderValue == that.orderValue ? 0 : 1));
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(chromosome);
        out.writeInt(position);
        out.writeInt(orderValue);
    }

    public void readFields(DataInput in) throws IOException {
        chromosome = in.readInt();
        position = in.readInt();
        orderValue = in.readInt();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("chrom: ").append(chromosome);
        sb.append(", pos: ").append(position);
        return sb.toString();
    }

}