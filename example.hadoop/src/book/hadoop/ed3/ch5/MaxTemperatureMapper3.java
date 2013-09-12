package book.hadoop.ed3.ch5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MaxTemperatureMapper3 extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static Logger logger = LoggerFactory.getLogger(MaxTemperatureMapper3.class);
	
	enum Temperature {
		OVER_100,
		MALFORMED
	}

	private NcdcRecordParser2 parser = new NcdcRecordParser2();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		parser.parse(value);

		if (parser.isValidTemperature()) {
			int airTemperature = parser.getAirTemperature();
			context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
			
			if (airTemperature > 1000) {   // > 1000
				logger.error("Temperature over 100 degrees for input: " + value);
				
				context.setStatus("Detected possibly corrupt record: see logs.");
				context.getCounter(Temperature.OVER_100).increment(1);
			}
		} else if (parser.isMalformedTemperature()) {
			logger.error("Ignoring possibly corrupt input: " + value);
			context.getCounter(Temperature.MALFORMED).increment(1);
		}
	}
}