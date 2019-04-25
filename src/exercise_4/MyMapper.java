package exercise_4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
		String currentLine = line.toString();
		String words[] = currentLine.split(" ");
		for (String word : words) {
			word = word.replaceAll("[^a-zA-Z]", "");
			System.out.println("Key: " + word);
			System.out.println("Value: 1");
			System.out.println();
			context.write(new Text(word), new IntWritable(1));
		}
	}

}
