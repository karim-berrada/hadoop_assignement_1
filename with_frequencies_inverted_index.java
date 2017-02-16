// Created my own package
package mypackage.stopwords;

//using libraries and functions for the format, mapper, reducer, ... 
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//using IOException to catch the exceptions 
import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
// Adding the ArrayList function so that we don't use the sets
import java.util.ArrayList;
// Added the collections function to count the frequencies
import java.util.Collections;
// Adding hashset util
import java.util.HashSet;

// Inverted Index Class
public class with_frequencies_inverted_index extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new with_frequencies_inverted_index(), args);

		System.exit(res);
	}

	@Override
	//job conf
	public int run(String[] args) throws Exception {
		// Initializing the job and the sys out
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "with_frequencies_inverted_index");
		// Initializing the key/values, mapper/reducer and the input/ouput format
		job.setJarByClass(with_frequencies_inverted_index.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// setting up the configuration
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
		// We use 10 reducers
		job.setNumReduceTasks(10);

		// take the given arguments and give them as the input and output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		job.waitForCompletion(true);

		return 0;
	}
	
	// Mapping class where we read the stopwords, get all the words and filter them
	public static class Map extends 
			Mapper<LongWritable, Text, Text, Text> {
		private Text file = new Text();
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// This is where we read the stopwords
			HashSet<String> stopwords = new HashSet<String>();
			BufferedReader csv_buffer = new BufferedReader(
					new FileReader(
							new File("/Users/kermousberrada/Documents/workspace/assignement_1_/stopwords.csv"))); 
			
			String line;
			// Going through and using lowercase again
			while ((line = csv_buffer.readLine()) != null) {
				stopwords.add(line.toLowerCase());}
			csv_buffer.close();
			
			// Here we catch the path and name
			String MyString = ((FileSplit) context.getInputSplit())
					.getPath().getName();
			file = new Text(MyString);

			// This is where we eliminates the stopwords and write them as (key, value)
			for (String token : value.toString().split("\\s+")) {
				if (!stopwords.contains(token.toLowerCase())) {
					word.set(token.toLowerCase());
					context.write(word, file);}
			}			
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		//Reducing class where we get the values and write the output
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			// Using an Array to keep duplicates
			ArrayList<String> MyList = new ArrayList<String>();
			
			// adding the values 
			for (Text value : values) {MyList.add(value.toString());}
									
			HashSet<String> set = new HashSet<String>(MyList);
			StringBuilder MyStringBuilder = new StringBuilder();
			
			// We build our output
			
			String prefix = "";
			// Looping on the values and checking if we already counted the # 
			// We also use the Collections.frequency on our list
			for (String value : set) {
				
				if (value.contains("#")){	
					// We split our value and count the frequency of our first split
					String[] MySplits = value.split("#");
					String first_split = MySplits[0]; 
					String second_split = MySplits[1];
					
					MyStringBuilder.append(prefix);
					prefix = ", ";
					MyStringBuilder.append(value + "#" + (Collections.frequency(MyList,first_split) + Integer.parseInt(second_split)) );
				}
				MyStringBuilder.append(prefix);
				prefix = ", ";
				// We intialize the counting of the frequency 
				MyStringBuilder.append(value + "#" + Collections.frequency(MyList,value));
			}

			context.write(key, new Text(MyStringBuilder.toString()));
		}
	}
}