package util;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 13.02.16.
 */
public class ChromPosKeyWritable implements Writable, WritableComparable<ChromPosKeyWritable> {
    private int chromosome;
    private int position;

    public ChromPosKeyWritable() {
    }

    public ChromPosKeyWritable(int chromosome, int position) {
        this.chromosome = chromosome;
        this.position = position;
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

    public void write(DataOutput out) throws IOException {
        out.writeInt(chromosome);
        out.writeInt(position);
    }

    public void readFields(DataInput in) throws IOException {
        chromosome = in.readInt();
        position = in.readInt();
    }

    public int compareTo(ChromPosKeyWritable that) {
        if(((Integer)this.chromosome).compareTo((Integer)that.chromosome) != 0) {
            return ((Integer)this.chromosome).compareTo(that.chromosome);
        } else {
            return (this.position<that.position ? -1 : (this.position==that.position ? 0 : 1));
        }
    }

    @Override
    public String toString() {
        return chromosome + "\t" + position;
    }

}