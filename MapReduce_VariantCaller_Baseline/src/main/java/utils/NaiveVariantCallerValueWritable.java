package utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 14.08.15.
 */
public class NaiveVariantCallerValueWritable implements Writable {

    private char mostDominantBase;

    public NaiveVariantCallerValueWritable(char mostDominantBase) {
        this.mostDominantBase = mostDominantBase;
    }

    public void write(DataOutput out) throws IOException {
        out.writeChar(this.mostDominantBase);
    }

    public void readFields(DataInput in) throws IOException {
        this.mostDominantBase = in.readChar();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NaiveVariantCallerValueWritable that = (NaiveVariantCallerValueWritable) o;
        return mostDominantBase == that.mostDominantBase;
    }

    @Override
    public int hashCode() {
        return (int) mostDominantBase;
    }

    @Override
    public String toString() {
        return String.valueOf(this.mostDominantBase);
    }

}