import org.apache.hadoop.io.NullWritable;
import util.ChromPosKey;
import util.JoinedResultWritable;

import java.io.IOException;
import java.util.Iterator;

/**
 * master-thesis Clemens Banas
 * Organization: DBIS - University of Innsbruck
 * Created 21.04.2016
 */
public class VCF_Join_Reducer extends org.apache.hadoop.mapreduce.Reducer<ChromPosKey, JoinedResultWritable, NullWritable, JoinedResultWritable> {

    @Override
    protected void reduce(ChromPosKey key, Iterable<JoinedResultWritable> values, Context context) throws IOException, InterruptedException {
        final Iterator<JoinedResultWritable> it = values.iterator();
        JoinedResultWritable leftTuple = null;
        if (it.hasNext()) {
            leftTuple = new JoinedResultWritable(it.next());
        }

        while (it.hasNext()) {
            final JoinedResultWritable rightTuple = new JoinedResultWritable(it.next());
            if (leftTuple.getOrderValue() == 0 && rightTuple.getOrderValue() == 1
                    && leftTuple.getChrom() == rightTuple.getChrom()
                    && leftTuple.getPos() == rightTuple.getPos()) {
                context.write(
                        null,
                        new JoinedResultWritable(
                                leftTuple.getOrderValue(),
                                leftTuple.getChrom(),
                                leftTuple.getPos(),
                                leftTuple.getId(),
                                leftTuple.getRef(),
                                leftTuple.getAlt(),
                                leftTuple.getQual(),
                                leftTuple.getFilter(),
                                leftTuple.getInfo() + ";" + rightTuple.getInfo(),
                                leftTuple.getFormat(),
                                leftTuple.getGenotypes()
                        )
                );
            }
            else if (leftTuple.getOrderValue() == 0 && leftTuple.hasGenotypes()) { //left outer join
                context.write(null, leftTuple);
            }
            leftTuple = rightTuple;
        }
    }

}