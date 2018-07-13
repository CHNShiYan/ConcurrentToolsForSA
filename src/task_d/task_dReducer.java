package task_d;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class task_dReducer extends Reducer<Text, Text, Text, Text>{

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
		
		Map<String,Integer> cnt=new HashMap<String,Integer>();
		Map<String,Double> sum=new HashMap<String,Double>();
		//设置迭代器
		Iterator<Text> iterator=values.iterator();
		while(iterator.hasNext()){
			String[] house_type_totalprice=iterator.next().toString().split(" ");
			String house_type=house_type_totalprice[0];
			Double house_totalprice=Double.parseDouble(house_type_totalprice[1]);
			if (cnt.containsKey(house_type))  
				cnt.put(house_type, cnt.get(house_type) + 1);  
	        else
	           	cnt.put(house_type, 1);  
			if (sum.containsKey(house_type))  
				sum.put(house_type, sum.get(house_type) + house_totalprice);  
	        else
	        	sum.put(house_type, house_totalprice);
		}
		for(String type : cnt.keySet()){
			double average=sum.get(type)/cnt.get(type);
			content.write(new Text(key+"\t"+type), new Text(String.valueOf(average)));
		}
	}

	

}
