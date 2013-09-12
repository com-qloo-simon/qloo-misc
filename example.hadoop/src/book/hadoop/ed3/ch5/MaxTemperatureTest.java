package book.hadoop.ed3.ch5;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mrunit.mapreduce.*;
import org.apache.hadoop.mapreduce.*;

import org.junit.*;

import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.*;
import static org.hamcrest.CoreMatchers.*;

import book.hadoop.ed3.ch2.MaxTemperatureReducer;


public class MaxTemperatureTest {
	static LongWritable key0 = new LongWritable(0);
	
	@Ignore("Not Ready to Run")
	@Test
	public void processesValidRecord() throws IOException, InterruptedException {

		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + 
									  // Year ^^^^
			"99999V0203201N00261220001CN9999999N9-00111+99999999999");
								  // Temperature ^^^^^

		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxTemperatureMapper2())
		.withInput(key0, value)
		.withOutput(new Text("1950"), new IntWritable(-11))
		.runTest();
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void ignoresMissingTemperatureRecord() throws IOException, InterruptedException {

		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + 
		                              // Year ^^^^
			"99999V0203201N00261220001CN9999999N9+99991+99999999999");
							      // Temperature ^^^^^

		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxTemperatureMapper2())
		.withInput(key0, value)
		.runTest();
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void parsesMalformedTemperature() throws IOException, InterruptedException {

		Text value = new Text("0335999999433181957042302005+37950+139117SAO +0004" +
									  // Year ^^^^
			"RJSN V02011359003150070356999999433201957010100005+353");
								  // Temperature ^^^^^

		Counters counters = new Counters();

		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxTemperatureMapper3())
		.withInput(key0, value)
		.withCounters(counters)
		.runTest();

		Counter c = counters.findCounter(MaxTemperatureMapper3.Temperature.MALFORMED);
		System.out.println("counter value: " + c.getValue());

		assertThat(c.getValue(), is(1L));
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void returnsMaximumIntegerInValues() throws IOException, InterruptedException {

		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		.withReducer(new MaxTemperatureReducer())
		.withInput(new Text("1950"), Arrays.asList(new IntWritable(10), new IntWritable(5)))
		.withOutput(new Text("1950"), new IntWritable(10))
		.runTest();
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void test() throws Exception {
		Configuration conf = new Configuration();
	
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
	
		Path input = new Path("input");
		Path output = new Path("output");
	
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true); // delete old output
	
		MaxTemperatureJob job = new MaxTemperatureJob();
		job.setConf(conf);
	
		int exitCode = job.run(new String[] {input.toString(), output.toString() });
	
		assertThat(exitCode, is(0));
	
		//checkOutput(conf, output);
	}
}