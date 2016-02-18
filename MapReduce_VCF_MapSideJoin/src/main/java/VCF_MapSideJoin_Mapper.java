import htsjdk.tribble.util.ParsingUtils;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import util.ChromPosKey;
import util.JoinedResultWritable;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 14.02.16.
 */
public class VCF_MapSideJoin_Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, VariantContextWritable, NullWritable, JoinedResultWritable> {

    private Map<ChromPosKey, JoinedResultWritable> vcfSampleMap = new HashMap<ChromPosKey, JoinedResultWritable>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        for (Path eachPath : cacheFilesLocal) {
            if (eachPath.getName().toString().trim().equals("20.filtered.PASS_noHeader.vcf")) {
                loadSmallVCFJoinPartnerHashMap(eachPath, context);
            }
        }
    }

    private void loadSmallVCFJoinPartnerHashMap(Path filePath, Context context) {
        String strLineRead = "";
        BufferedReader brReader = null;
        try {
            brReader = new BufferedReader(new FileReader(filePath.toString()));
            while ((strLineRead = brReader.readLine()) != null) {
                String vcfLineArray[] = strLineRead.split("\t");
                int chrom = Integer.valueOf(vcfLineArray[0].trim());
                int pos = Integer.valueOf(vcfLineArray[1].trim());

                vcfSampleMap.put(
                        new ChromPosKey(chrom, pos),
                        new JoinedResultWritable(
                                chrom,
                                pos,
                                vcfLineArray[2].trim(),
                                vcfLineArray[3].trim().charAt(0),
                                vcfLineArray[4].trim().charAt(0),
                                Integer.valueOf(vcfLineArray[5].trim()),
                                vcfLineArray[6].trim(),
                                vcfLineArray[7]
                        )
                );
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (brReader != null) {
                try {
                    brReader.close();
                } catch (IOException e) {
                    // ignore exception
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, VariantContextWritable value, Context context) throws IOException, InterruptedException {
        final VariantContext variantContext = value.get();
        final ChromPosKey chromPosKey = new ChromPosKey(Integer.valueOf(variantContext.getContig()), variantContext.getStart());
        if (vcfSampleMap.containsKey(chromPosKey)) {
            final JoinedResultWritable res = vcfSampleMap.get(chromPosKey);
            res.setId(variantContext.getID());
            res.setInfo(ParsingUtils.sortedString(variantContext.getAttributes()));
            context.write(null, res);
        }
    }

}