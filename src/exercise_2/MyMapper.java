package exercise_2;

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
		String gender = words[1];
		int salary = Integer.parseInt(words[2]);
		System.out.println("Gender: " + gender);
		System.out.println("Salary: " + salary);
		System.out.println();
		context.write(new Text(gender), new IntWritable(salary));
	}

}
