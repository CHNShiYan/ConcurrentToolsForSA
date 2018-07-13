package task_g;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class task_gMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//对输入的数据首先按行进行分割
				String line=value.toString();
				StringTokenizer tokenizer = new StringTokenizer(line, "\n");
				//分别对每一行进行处理
				while(tokenizer.hasMoreTokens()){
					//每行按制表符划分
					StringTokenizer tokenizerLine=new StringTokenizer(tokenizer.nextToken(),"\t");
					String house_type=tokenizerLine.nextToken();//区域部分
					String house_price=tokenizerLine.nextToken();//总价部分
					if (house_price=="-1") {
						continue;
					}
					Text name=new Text(house_type);
					Long house_priceLong=Long.parseLong(house_price);
					context.write(name, new LongWritable(house_priceLong));
				}
	}

	

}
