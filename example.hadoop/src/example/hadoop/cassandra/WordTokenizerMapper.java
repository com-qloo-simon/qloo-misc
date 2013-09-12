package example.hadoop.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SortedMap;
import java.util.StringTokenizer;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordTokenizerMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private ByteBuffer sourceColumn;
    
    String punctuationsToStrip[] = { "\"", "'", ",", ";", "!", ":", "\\?", "\\.", "\\(", "\\-", "\\[", "\\)", "\\]" };

    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        sourceColumn = ByteBufferUtil.bytes(context.getConfiguration().get("columnname"));         
    }

    public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException {         
        // Our slice predicate contains only one column. We fetch it here
        IColumn column = columns.get(sourceColumn);
        if (column == null)
            return;
        
        String value = ByteBufferUtil.string(column.value());
        value = value.toLowerCase();
        
        for (String pattern : punctuationsToStrip) {
          value = value.replaceAll(pattern, "");
        }            

        StringTokenizer st = new StringTokenizer(value);
        while (st.hasMoreTokens()) {
        	String s = st.nextToken();
        	try {
        		Integer.parseInt(s);
        	} catch (Throwable th) {
        		word.set(s);
        		context.write(word, one);
        	}
        }
    }
}
