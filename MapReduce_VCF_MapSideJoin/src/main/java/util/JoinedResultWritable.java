package util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 14.02.16.
 */
public class JoinedResultWritable implements Writable {
    private int chrom;
    private int pos;
    private String id;
    private char ref;
    private char alt;
    private int qual;
    private String filter;
    private String info;

    public JoinedResultWritable() {
    }

    public JoinedResultWritable(int chrom, int pos, String id, char ref, char alt, int qual, String filter, String info) {
        this.chrom = chrom;
        this.pos = pos;
        this.id = id;
        this.ref = ref;
        this.alt = alt;
        this.qual = qual;
        this.filter = filter;
        this.info = info;
    }

    public int getChrom() {
        return chrom;
    }

    public void setChrom(int chrom) {
        this.chrom = chrom;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public char getRef() {
        return ref;
    }

    public void setRef(char ref) {
        this.ref = ref;
    }

    public char getAlt() {
        return alt;
    }

    public void setAlt(char alt) {
        this.alt = alt;
    }

    public int getQual() {
        return qual;
    }

    public void setQual(int qual) {
        this.qual = qual;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JoinedResultWritable that = (JoinedResultWritable) o;

        if (chrom != that.chrom) return false;
        if (pos != that.pos) return false;
        if (ref != that.ref) return false;
        if (alt != that.alt) return false;
        if (qual != that.qual) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (filter != null ? !filter.equals(that.filter) : that.filter != null) return false;
        return info != null ? info.equals(that.info) : that.info == null;

    }

    @Override
    public int hashCode() {
        int result = chrom;
        result = 31 * result + pos;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (int) ref;
        result = 31 * result + (int) alt;
        result = 31 * result + qual;
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        result = 31 * result + (info != null ? info.hashCode() : 0);
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(chrom);
        out.writeInt(pos);
        out.writeUTF(id);
        out.writeChar(ref);
        out.writeChar(alt);
        out.writeInt(qual);
        out.writeUTF(filter);
        out.writeUTF(info);
    }

    public void readFields(DataInput in) throws IOException {
        chrom = in.readInt();
        pos = in.readInt();
        id = in.readUTF();
        ref = in.readChar();
        alt = in.readChar();
        qual = in.readInt();
        filter = in.readUTF();
        info = in.readUTF();
    }

    @Override
    public String toString() {
        final String delimiter = "\t";
        final StringBuilder sb = new StringBuilder();
        sb.append(chrom);
        sb.append(delimiter);
        sb.append(pos);
        sb.append(delimiter);
        sb.append(id);
        sb.append(delimiter);
        sb.append(ref);
        sb.append(delimiter);
        sb.append(alt);
        sb.append(delimiter);
        sb.append(qual);
        sb.append(delimiter);
        sb.append(filter);
        sb.append(delimiter);
        sb.append(info);
        return sb.toString();
    }
}