package example.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.qloo.data.util.UUIDUtil;
 

public class UUIDJobTest { 
	MapDriver<LongWritable, Text, BytesWritable, IntWritable> mapDriver;
	ReduceDriver<BytesWritable, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, BytesWritable, IntWritable, Text, IntWritable> mapReduceDriver;
 
	@Before
	public void setUp() {
	    UUIDMapper mapper = new UUIDMapper();
	    UUIDReducer reducer = new UUIDReducer();
	    mapDriver = MapDriver.newMapDriver(mapper);;
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
 
	@Test
	public void testMapper() {
	    mapDriver.withInput(new LongWritable(), new Text("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2".toLowerCase()));
	    
	    byte[] byteArray = UUIDUtil.toByteArray(UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2".toLowerCase()));
	    
	    BytesWritable bw = new BytesWritable();
	    
	    bw.set(byteArray, 0, byteArray.length);
	    
	    mapDriver.withOutput(bw, new IntWritable(1));
	    
	    try {
	    	mapDriver.runTest();
	    } catch (IOException ioe) {
	    	ioe.printStackTrace();
	    }
	}
 
	// @Ignore("Not Ready to Run")
	@Test
	public void testReducer() {
	    byte[] byteArray = UUIDUtil.toByteArray(UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2".toLowerCase()));
	    
	    BytesWritable bw = new BytesWritable();
	    
	    bw.set(byteArray, 0, byteArray.length);
		
	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    
	    reduceDriver.withInput(bw, values);
	    reduceDriver.withOutput(new Text("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2".toLowerCase()), new IntWritable(2));
	    
	    try {
	    	reduceDriver.runTest();
	    } catch (IOException ioe) {
	    	ioe.printStackTrace();
	    }
	}
}