package util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */
public class JoinedResultWritable implements Writable {
    //only needed to see at the reducer from which
    //file the record is originating from
    private int orderValue;

    private int chrom;
    private int pos;
    private String id;
    private String ref;
    private String alt;
    private String qual;
    private String filter;
    private String info;
    private String format;
    private String genotypes;

    public JoinedResultWritable() {

    }

    public JoinedResultWritable(int orderValue, int chrom, int pos, String id, String ref, String alt, String qual, String filter, String info, String format, String genotypes) {
        this.orderValue = orderValue;
        this.chrom = chrom;
        this.pos = pos;
        this.id = id;
        this.ref = ref;
        this.alt = alt;
        this.qual = qual;
        this.filter = filter;
        this.info = info;
        this.format = format;
        this.genotypes = genotypes;
    }

    //copy constructor
    public JoinedResultWritable(JoinedResultWritable other) {
        this.orderValue = other.orderValue;
        this.chrom = other.chrom;
        this.pos = other.pos;
        this.id = other.id;
        this.ref = other.ref;
        this.alt = other.alt;
        this.qual = other.qual;
        this.filter = other.filter;
        this.info = other.info;
        this.format = other.format;
        this.genotypes = other.genotypes;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(orderValue);
        out.writeInt(chrom);
        out.writeInt(pos);
        out.writeUTF(id);
        out.writeUTF(ref);
        out.writeUTF(alt);
        out.writeUTF(qual);
        out.writeUTF(filter);
        out.writeUTF(info);
        out.writeUTF(format);
        out.writeUTF(genotypes);
    }

    public void readFields(DataInput in) throws IOException {
        orderValue = in.readInt();
        chrom = in.readInt();
        pos = in.readInt();
        id = in.readUTF();
        ref = in.readUTF();
        alt = in.readUTF();
        qual = in.readUTF();
        filter = in.readUTF();
        info = in.readUTF();
        format = in.readUTF();
        genotypes = in.readUTF();
    }

    public int getOrderValue() {
        return orderValue;
    }

    public int getChrom() {
        return chrom;
    }

    public int getPos() {
        return pos;
    }

    public String getId() {
        return id;
    }

    public String getRef() {
        return ref;
    }

    public String getAlt() {
        return alt;
    }

    public String getQual() {
        return qual;
    }

    public String getFilter() {
        return filter;
    }

    public String getInfo() {
        return info;
    }

    public String getFormat() {
        return format;
    }

    public String getGenotypes() {
        return genotypes;
    }

    public boolean hasGenotypes() {
        return !this.genotypes.equals("");
    }

    @Override
    public String toString() {
        final char delimiter = '\t';
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
        sb.append(info.trim());
        sb.append(delimiter);
        sb.append(format);
        sb.append(delimiter);
        sb.append(genotypes.trim());
        return sb.toString();
    }

}