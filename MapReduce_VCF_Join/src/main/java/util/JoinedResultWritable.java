package util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 25.03.16.
 */
public class JoinedResultWritable implements Writable {
    private int chrom;
    private int pos;
    private String id;
    private char ref;
    private char alt;
    private String qual;
    private String filter;
    private String info;
    private String genotypes;
    private String infoRef;

    public JoinedResultWritable() {

    }

    public JoinedResultWritable(int chrom, int pos, String id, char ref, char alt, String qual, String filter, String info, String genotypes, String infoRef) {
        this.chrom = chrom;
        this.pos = pos;
        this.id = id;
        this.ref = ref;
        this.alt = alt;
        this.qual = qual;
        this.filter = filter;
        this.info = info;
        this.genotypes = genotypes;
        this.infoRef = infoRef;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(chrom);
        out.writeInt(pos);
        out.writeUTF(id);
        out.writeChar(ref);
        out.writeChar(alt);
        out.writeUTF(qual);
        out.writeUTF(filter);
        out.writeUTF(info);
        out.writeUTF(genotypes);
        out.writeUTF(infoRef);
    }

    public void readFields(DataInput in) throws IOException {
        chrom = in.readInt();
        pos = in.readInt();
        id = in.readUTF();
        ref = in.readChar();
        alt = in.readChar();
        qual = in.readUTF();
        filter = in.readUTF();
        info = in.readUTF();
        genotypes = in.readUTF();
        infoRef = in.readUTF();
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
        sb.append(info);
        sb.append(delimiter);
        sb.append(infoRef);
        sb.append(delimiter);
        sb.append(genotypes);
        sb.append(delimiter);
        return sb.toString();
    }

}