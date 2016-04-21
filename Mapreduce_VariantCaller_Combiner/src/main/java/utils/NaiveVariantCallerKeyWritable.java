package utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */
public class NaiveVariantCallerKeyWritable implements WritableComparable<NaiveVariantCallerKeyWritable> {
    private String sampleIdentifier;
    private int position;

    public NaiveVariantCallerKeyWritable() {

    }

    public NaiveVariantCallerKeyWritable(String sampleIdentifier, int position) {
        this.sampleIdentifier = sampleIdentifier;
        this.position = position;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(position);
        out.writeUTF(sampleIdentifier);
    }

    public void readFields(DataInput in) throws IOException {
        position = in.readInt();
        sampleIdentifier = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NaiveVariantCallerKeyWritable that = (NaiveVariantCallerKeyWritable) o;

        if (position != that.position) return false;
        return !(sampleIdentifier != null ? !sampleIdentifier.equals(that.sampleIdentifier) : that.sampleIdentifier != null);

    }

    @Override
    public int hashCode() {
        int result = sampleIdentifier != null ? sampleIdentifier.hashCode() : 0;
        result = 31 * result + position;
        return result;
    }

    public String getSampleIdentifier() {
        return sampleIdentifier;
    }

    public void setSampleIdentifier(String sampleIdentifier) {
        this.sampleIdentifier = sampleIdentifier;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return this.sampleIdentifier + "," + this.position;
    }

    /** Compares two NaiveVariantCallerKeyWritables. */
    public int compareTo(NaiveVariantCallerKeyWritable that) {
        if(this.sampleIdentifier.compareTo(that.sampleIdentifier) != 0) {
            return this.sampleIdentifier.compareTo(that.sampleIdentifier);
        } else {
            return (this.position<that.position ? -1 : (this.position==that.position ? 0 : 1));
        }
    }

    public static class NaiveVariantCallerKeyWritableComparator extends WritableComparator {
        public NaiveVariantCallerKeyWritableComparator() {
            super(NaiveVariantCallerKeyWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static { // register this comparator
        WritableComparator.define(NaiveVariantCallerKeyWritable.class,
                new NaiveVariantCallerKeyWritableComparator());
    }
}