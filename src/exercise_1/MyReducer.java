package exercise_1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Iterable<IntWritable>, Text, IntWritable> {

	@Override
	protected void reduce(Text avg, Iterable<Iterable<IntWritable>> value, Context ctx)
			throws IOException, InterruptedException {
		System.out.print("Average: " + avg + " :: Count:");
		Iterator it = value.iterator();
		int total_count = 0;
		double sum = 0;
		while (it.hasNext()) {
			IntWritable i = (IntWritable) it.next();
			sum += Double.parseDouble(avg.toString()) * i.get();
			total_count += i.get();
			System.out.print(" " + i.get());
		}
		System.out.println();
		double average = sum / total_count;
		System.out.println("Sum: " + sum);
		System.out.println("Total Count: " + total_count);
		System.out.println();
		ctx.write(new Text(Double.toString(average)), new IntWritable(total_count));
	}

}
