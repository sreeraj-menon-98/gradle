package exercise_1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
		String currentLine = line.toString();
		String values[] = currentLine.split(" ");
		int sum = 0, count = 0;
		System.out.print("Values: ");
		for (String value : values) {
			sum += Integer.parseInt(value);
			count++;
			System.out.print(value + " ");
		}
		double avg = sum / count;
		System.out.println();
		System.out.println("Average: " + avg);
		System.out.println("Count: " + count);
		System.out.println();
		context.write(new Text(Double.toString(avg)), new IntWritable(count));
	}

}
