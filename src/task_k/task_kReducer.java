package task_k;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class task_kReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
	
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context content) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum=0;
		int count=0;
		int house_price=0;
		//设置迭代器
		Iterator<Text> iterator=values.iterator();
		while(iterator.hasNext()){
			house_price=Integer.parseInt(iterator.next().toString());
			sum+=house_price;
			count++;
		}
		double average =1.0*sum/count;
		content.write(key, new Text(String.valueOf(average)));
	}

	

}
