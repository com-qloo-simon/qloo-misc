package misc;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This counts the occurrences of words in a ColumnFamily, that has a single column (that we care about)
 * containing a sequence of words.
 *
 * For each word, we output the total number of occurrences across all texts.
 *
 * When outputting to Cassandra, we write the word counts as a {word, count} column/value pair,
 * with a row key equal to the name of the source column we read the words from.
 */

public class WordCountJob extends Configured implements Tool {

	public int run(String[] args) throws Exception {
	    String columnName = "name";
	    getConf().set("columnname", columnName);
	
	    Job job = new Job(getConf(), "wordcount");
	    job.setJarByClass(WordCountJob.class);
	    
	    job.setMapperClass(WordTokenizerMapper.class);
	    // Tell the Mapper to expect Cassandra columns as input
	    job.setInputFormatClass(ColumnFamilyInputFormat.class); 
	    // Tell the "Shuffle/Sort" phase of M/R what type of Key/Value to expect from the mapper        
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	
	    job.setReducerClass(WordCountReducer.class);
	    job.setOutputFormatClass(ColumnFamilyOutputFormat.class);        
	    job.setOutputKeyClass(ByteBuffer.class);
	    job.setOutputValueClass(List.class);
	
	    // Set the keyspace and column family for the output of this job
	    ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "qloo_b3", "output_words");
	    // Set the keyspace and column family for the input of this job
	    ConfigHelper.setInputColumnFamily(job.getConfiguration(), "qloo_b3", "profile");
	    
	    ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
	    ConfigHelper.setInputInitialAddress(job.getConfiguration(), "10.136.11.98,10.151.43.81");
	    ConfigHelper.setInputPartitioner(job.getConfiguration(), "RandomPartitioner");
	    
	    ConfigHelper.setOutputRpcPort(job.getConfiguration(), "9160");
	    ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "10.136.11.98,10.151.43.81");
	    ConfigHelper.setOutputPartitioner(job.getConfiguration(), "RandomPartitioner");
	  
	    // Set the predicate that determines what columns will be selected from each row
	    SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes(columnName)));
	    // The "get_slice" (see Cassandra's API) operation will be applied on each row of the ColumnFamily.
	    // Each row will be handled by one Map job.
	    ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
	
	    job.waitForCompletion(true);
	    return job.isSuccessful() ? 0:1;
	}
	
	public static void main(String[] args) throws Exception {
	    // Let ToolRunner handle generic command-line options
	    ToolRunner.run(new Configuration(), new WordCountJob(), args);
	    System.exit(0);
	}    
}
