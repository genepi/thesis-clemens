package pigGene.storage.reference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;

public class PigGeneStorageReferenceFile extends PigStorage {
	static ResourceSchema schema;

	static {
		final ArrayList<FieldSchema> fieldSchemaList = new ArrayList<FieldSchema>();
		fieldSchemaList.add(new FieldSchema("chrom", org.apache.pig.data.DataType.CHARARRAY));
		fieldSchemaList.add(new FieldSchema("pos", org.apache.pig.data.DataType.LONG));
		fieldSchemaList.add(new FieldSchema("id", org.apache.pig.data.DataType.CHARARRAY));
		fieldSchemaList.add(new FieldSchema("ref", org.apache.pig.data.DataType.CHARARRAY));
		fieldSchemaList.add(new FieldSchema("alt", org.apache.pig.data.DataType.CHARARRAY));
		fieldSchemaList.add(new FieldSchema("qual", org.apache.pig.data.DataType.DOUBLE));
		fieldSchemaList.add(new FieldSchema("filt", org.apache.pig.data.DataType.CHARARRAY));
		fieldSchemaList.add(new FieldSchema("info", org.apache.pig.data.DataType.CHARARRAY));
		schema = new ResourceSchema(new Schema(fieldSchemaList));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() {
		return new PigGeneInputFormatReferenceFile();
	}

	@Override
	public ResourceSchema getSchema(final String location, final Job job) throws IOException {
		final Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[] { signature });
		p.setProperty(signature + ".schema", schema.toString());
		return schema;
	}

}