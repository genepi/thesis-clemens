package util;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 12.02.16.
 */
public class ChromPositionOrderKey implements WritableComparable<ChromPositionOrderKey> {
    private int chromosome;
    private int position;

    private int joinOrder;

    public ChromPositionOrderKey() {
    }

    public ChromPositionOrderKey(int chromosome, int position, int joinOrder) {
        this.chromosome = chromosome;
        this.position = position;
        this.joinOrder = joinOrder;
    }

    public ChromPositionOrderKey(int chromosome, int position) {
        this(chromosome, position, 0);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(chromosome);
        out.writeInt(position);
        out.writeInt(joinOrder);
    }

    public void readFields(DataInput in) throws IOException {
        chromosome = in.readInt();
        position = in.readInt();
        joinOrder = in.readInt();
    }

    public int getChromosome() {
        return chromosome;
    }

    public int getPosition() {
        return position;
    }

    public int getJoinOrder() {
        return joinOrder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChromPositionOrderKey that = (ChromPositionOrderKey) o;

        if (chromosome != that.chromosome) return false;
        if (position != that.position) return false;
        return joinOrder == that.joinOrder;

    }

    @Override
    public int hashCode() {
        int result = chromosome;
        result = 31 * result + position;
        result = 31 * result + joinOrder;
        return result;
    }

    public int compareTo(ChromPositionOrderKey that) {
        int result = 0;
        if (this.getChromosome() != that.getChromosome()) {
            result = (this.getChromosome() < that.getChromosome() ? -1 : 1);
        } else if (this.getPosition() != that.getPosition()) {
            result = (this.getPosition() < that.getPosition() ? -1 : 1);
        }
        if (0 == result) {
            result = (this.getJoinOrder() < that.getJoinOrder() ? -1 : (this.getJoinOrder() == that.getJoinOrder() ? 0 : 1));
        }
        return result;
    }

    @Override
    public String toString() {
        return chromosome + "\t" + position;
    }

}