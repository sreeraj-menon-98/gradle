package exercise_2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Iterable<IntWritable>, Text, IntWritable> {

	@Override
	protected void reduce(Text gender, Iterable<Iterable<IntWritable>> value, Context ctx)
			throws IOException, InterruptedException {
		System.out.print("Gender: " + gender + " :: Salary:");
		Iterator it = value.iterator();
		int sum = 0, total = 0;
		while (it.hasNext()) {
			int salary = ((IntWritable) it.next()).get();
			sum += salary;
			total++;
			System.out.print(" " + salary);
		}
		System.out.println();
		double average = sum / total;
		System.out.println("Total Salary: " + sum);
		System.out.println("Average Salary: " + average);
		System.out.println();
	}

}
