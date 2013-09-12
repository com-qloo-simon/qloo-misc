package example.hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.qloo.data.util.UUIDUtil;


public class UUIDReducer extends Reducer<BytesWritable, IntWritable, Text, IntWritable> {
	private Text txt = new Text();
	
	protected void reduce(BytesWritable key, Iterable<IntWritable> values, Context context) throws java.io.IOException, InterruptedException {
		int sum = 0;
		
		for (IntWritable value : values) {
			sum += value.get();
		}
		
		byte[] byteArray = key.getBytes();
		
		txt.set(UUIDUtil.uuid(byteArray).toString());
		
		context.write(txt, new IntWritable(sum));
	}
}