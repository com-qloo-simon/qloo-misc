package example.hadoop;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.qloo.data.util.UUIDUtil;


public class UUIDMapper extends Mapper<LongWritable, Text, BytesWritable, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private BytesWritable bw = new BytesWritable();

    protected void setup(Mapper<LongWritable, Text, BytesWritable, IntWritable>.Context context) throws IOException, InterruptedException {     
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {         
        UUID uid = UUID.fromString(value.toString());
        
        byte[] byteArray = UUIDUtil.toByteArray(uid);
        
        bw.set(byteArray, 0, byteArray.length);
        
        context.write(bw, one);
    }
}
