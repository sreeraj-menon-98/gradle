package exercise_5;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Iterable<IntWritable>, Text, IntWritable> {

	@Override
	protected void reduce(Text word, Iterable<Iterable<IntWritable>> value, Context ctx)
			throws IOException, InterruptedException {
		System.out.print("Key: " + word + " :: Value:");
		Iterator it = value.iterator();
		int count = 0;
		while (it.hasNext()) {
			IntWritable i = (IntWritable) it.next();
			count += i.get();
			System.out.print(" " + i.get());
		}
		System.out.println();
		System.out.println("Word: " + word);
		System.out.println("Count: " + count);
		System.out.println();
		ctx.write(word, new IntWritable(count));
	}

}
