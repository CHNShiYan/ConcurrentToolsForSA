package task_g;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class task_gReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	@Override
	protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
	
	@Override
	protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context content) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Long sum=0L;
		Long count=0L;
		Long house_price=0L;
		//设置迭代器
		Iterator<LongWritable> iterator=values.iterator();
		while(iterator.hasNext()){
			house_price=iterator.next().get();
			sum+=house_price;
			count++;
		}
		Long average =(Long)sum/count;
		content.write(key, new LongWritable(average));
	}

	

}
