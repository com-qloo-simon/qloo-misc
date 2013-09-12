package example.hadoop.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import example.hadoop.SMSCDRReducer;


public class WordCountJobTest { 
	MapDriver<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable, Text, IntWritable> mapReduceDriver;
 
	@Before
	public void setUp() {
	    WordTokenizerMapper mapper = new WordTokenizerMapper();
	    SMSCDRReducer reducer = new SMSCDRReducer();
	    mapDriver = MapDriver.newMapDriver(mapper);;
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	    
	    String[] strings = mapDriver.getConfiguration().getStrings("io.serializations");
	    for (int i = 0; i < strings.length; i++)
	    	System.out.println(strings[i]);
	    
	    Configuration conf = new Configuration();
	    conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization," 
	                + "org.apache.hadoop.io.serializer.WritableSerialization");
	    //mapDriver.withConfiguration(conf);
	    //mapDriver.setOutputSerializationConfiguration(conf);
	    mapDriver.setConfiguration(conf);
	    
	    String[] strings2 = mapDriver.getConfiguration().getStrings("io.serializations");
	    for (int i = 0; i < strings2.length; i++)
	    	System.out.println(strings2[i]);
	}
 
	@Test
	public void testMapper() {
		//UUID uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2");
		ByteBuffer key = ByteBufferUtil.bytes("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2");
		
		SortedMap<ByteBuffer, IColumn> sm = new TreeMap<ByteBuffer, IColumn>();
		ByteBuffer colName = ByteBufferUtil.bytes("name");
		
		IColumn col = new Column(colName, ByteBufferUtil.bytes("John Smith"));
		sm.put(colName, col);
		
	    mapDriver.withInput(key, sm);
	    mapDriver.withOutput(new Text("john"), new IntWritable(1));
	    mapDriver.withOutput(new Text("smith"), new IntWritable(1));
	    
	    try {
	    	mapDriver.runTest();
	    } catch (IOException ioe) {
	    	ioe.printStackTrace();
	    }
	}
}