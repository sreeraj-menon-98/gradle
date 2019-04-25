package exercise_5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "myFifthExerciseJob");
		job.setJarByClass(MyDriver.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		Path inputDir = new Path("hdfs://localhost:9000/input_data_4/");
		FileInputFormat.addInputPath(job, inputDir);
		Path outputDir = new Path("hdfs://localhost:9000/output_data_5/");
		FileOutputFormat.setOutputPath(job, outputDir);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
