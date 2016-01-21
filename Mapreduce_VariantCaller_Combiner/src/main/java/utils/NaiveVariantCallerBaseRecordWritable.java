package utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.01.16.
 */
public class NaiveVariantCallerBaseRecordWritable implements Writable {
    private static final char BASE_A = 'A';
    private static final char BASE_C = 'C';
    private static final char BASE_G = 'G';
    private static final char BASE_T = 'T';

    private int noOf_BASE_A;
    private int noOf_BASE_C;
    private int noOf_BASE_G;
    private int noOf_BASE_T;

    public NaiveVariantCallerBaseRecordWritable() {

    }

    public NaiveVariantCallerBaseRecordWritable(char base) {
        switch (base) {
            case BASE_A:
                this.noOf_BASE_A = 1; break;
            case BASE_C:
                this.noOf_BASE_C = 1; break;
            case BASE_G:
                this.noOf_BASE_G = 1; break;
            case BASE_T:
                this.noOf_BASE_T = 1; break;
            default:
                break;
        }
    }

    public NaiveVariantCallerBaseRecordWritable(int noOf_BASE_A, int noOf_BASE_C, int noOf_BASE_G, int noOf_BASE_T) {
        this.noOf_BASE_A = noOf_BASE_A;
        this.noOf_BASE_C = noOf_BASE_C;
        this.noOf_BASE_G = noOf_BASE_G;
        this.noOf_BASE_T = noOf_BASE_T;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(noOf_BASE_A);
        out.writeInt(noOf_BASE_C);
        out.writeInt(noOf_BASE_G);
        out.writeInt(noOf_BASE_T);
    }

    public void readFields(DataInput in) throws IOException {
        noOf_BASE_A = in.readInt();
        noOf_BASE_C = in.readInt();
        noOf_BASE_G = in.readInt();
        noOf_BASE_T = in.readInt();
    }

    public int getNoOf_BASE_A() {
        return noOf_BASE_A;
    }

    public int getNoOf_BASE_C() {
        return noOf_BASE_C;
    }

    public int getNoOf_BASE_G() {
        return noOf_BASE_G;
    }

    public int getNoOf_BASE_T() {
        return noOf_BASE_T;
    }

}