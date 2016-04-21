package utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */
public class NaiveVariantCallerPosition implements Writable {
    private int base_A;
    private int base_C;
    private int base_G;
    private int base_T;

    public NaiveVariantCallerPosition() {
        this.base_A = 0;
        this.base_C = 0;
        this.base_G = 0;
        this.base_T = 0;
    }

    public NaiveVariantCallerPosition(int base_A, int base_C, int base_G, int base_T) {
        this.base_A = base_A;
        this.base_C = base_C;
        this.base_G = base_G;
        this.base_T = base_T;
    }

    public int getNumberOfBase_A() {
        return base_A;
    }

    public void getNumberOfBase_A(int base_A) {
        this.base_A = base_A;
    }

    public int getNumberOfBase_C() {
        return base_C;
    }

    public void getNumberOfBase_C(int base_C) {
        this.base_C = base_C;
    }

    public int getNumberOfBase_G() {
        return base_G;
    }

    public void getNumberOfBase_G(int base_G) {
        this.base_G = base_G;
    }

    public int getNumberOfBase_T() {
        return base_T;
    }

    public void getNumberOfBase_T(int base_T) {
        this.base_T = base_T;
    }

    public void incrementBase_A() {
        this.base_A++;
    }

    public void incrementBase_C() {
        this.base_C++;
    }

    public void incrementBase_G() {
        this.base_G++;
    }

    public void incrementBase_T() {
        this.base_T++;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(base_A);
        out.writeInt(base_C);
        out.writeInt(base_G);
        out.writeInt(base_T);
    }

    public void readFields(DataInput in) throws IOException {
        base_A = in.readInt();
        base_C = in.readInt();
        base_G = in.readInt();
        base_T = in.readInt();
    }

}