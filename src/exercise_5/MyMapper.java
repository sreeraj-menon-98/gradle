package exercise_5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private String str = "";

	@Override
	protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
		String currentLine = line.toString().replaceAll("[^a-zA-Z]", "");
		int i;
		String word;
		if (str != "") {
			word = str + currentLine.charAt(0);
			System.out.println("Key: " + word);
			System.out.println("Value: 1");
			System.out.println();
			context.write(new Text(word), new IntWritable(1));
			
			word = "";
			word = word + str.charAt(1) + currentLine.charAt(0) + currentLine.charAt(1);
			System.out.println("Key: " + word);
			System.out.println("Value: 1");
			System.out.println();
			context.write(new Text(word), new IntWritable(1));
		}
		for (i = 0; i < currentLine.length() - 2; i++) {
			word = "";
			word = word + currentLine.charAt(i) + currentLine.charAt(i + 1) + currentLine.charAt(i + 2);
			System.out.println("Key: " + word);
			System.out.println("Value: 1");
			System.out.println();
			context.write(new Text(word), new IntWritable(1));
		}
		
		str = "" + currentLine.charAt(i) + currentLine.charAt(i + 1);
	}

}
