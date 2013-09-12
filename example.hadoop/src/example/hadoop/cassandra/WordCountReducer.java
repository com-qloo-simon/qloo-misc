package example.hadoop.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WordCountReducer extends Reducer<Text, IntWritable, ByteBuffer, List<Mutation>> {
	private static Logger logger = LoggerFactory.getLogger(WordCountReducer.class);
	
	private static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder();
	
    private ByteBuffer outputKey;

    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        // The row key is the name of the column from which we read the text
        outputKey = ByteBufferUtil.bytes(context.getConfiguration().get("columnname"));
    }

    public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        if (asciiEncoder.canEncode(word.toString())) {
        	context.write(outputKey, Collections.singletonList(getMutation(word, sum)));
        } else {
        	logger.error("not to write text: " + word + " with sum: " + sum);
        }
    }

    // See Cassandra API (http://wiki.apache.org/cassandra/API)
    private static Mutation getMutation(Text word, int sum) {
        Column c = new Column();
        c.setName(Arrays.copyOf(word.getBytes(), word.getLength()));
        c.setValue(ByteBufferUtil.bytes(String.valueOf(sum)));
        c.setTimestamp(System.currentTimeMillis());

        Mutation m = new Mutation();
        m.setColumn_or_supercolumn(new ColumnOrSuperColumn());
        m.column_or_supercolumn.setColumn(c);
        return m;
    }
}
