package task_f;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class task_fMapper extends Mapper<Object, Text, Text, Text>{

	@Override
	protected void setup(Mapper<Object, Text, Text,  Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
	
	@Override
	protected void cleanup(Mapper<Object, Text, Text,  Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//对输入的数据首先按行进行分割
		String line=value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, "\n");
		//分别对每一行进行处理
		while(tokenizer.hasMoreTokens()){
			//每行按制表符划分
			StringTokenizer tokenizerLine=new StringTokenizer(tokenizer.nextToken(),"\t");
			String house_propertyright=tokenizerLine.nextToken();//产权年限
			String house_area=tokenizerLine.nextToken();//区域部分
			String house_totalprice=tokenizerLine.nextToken();//总价部分
			System.out.println(house_propertyright+house_area+house_totalprice);
			if (!house_totalprice.equals("-1")) {
				if (house_propertyright.equals("1")|| house_propertyright.equals("0")) {
					Text name=new Text(house_propertyright);
					context.write(name, new Text(house_area+" "+house_totalprice));
				}
				else
				{
					if (house_propertyright.equals("2")) {
						Text name=new Text("0");
						context.write(name, new Text(house_area+" "+house_totalprice));
						Text name1=new Text("1");
						context.write(name1, new Text(house_area+" "+house_totalprice));
					}
				}
			}
			
		}
		
	}

	

}
