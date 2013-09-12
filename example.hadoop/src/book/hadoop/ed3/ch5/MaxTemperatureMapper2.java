package book.hadoop.ed3.ch5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MaxTemperatureMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
	enum Temperature {
		OVER_100
	};
	
	private NcdcRecordParser parser = new NcdcRecordParser();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		parser.parse(value);

		if (parser.isValidTemperature()) {
			int airTemperature = parser.getAirTemperature();
			if (airTemperature > 1000) {   // > 1000
				System.err.println("Temperature over 100 degrees for input: " + value);
				
				context.setStatus("Detected possibly corrupt record: see logs.");
				context.getCounter(Temperature.OVER_100).increment(1);
			}
			
			context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
		}
	}
}