package task_j;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class task_jMapper extends Mapper<Object, Text, Text, Text>{

	@Override
	protected void setup(Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
	
	@Override
	protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
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
					StringTokenizer tokenizerLine=new StringTokenizer(tokenizer.nextToken()," ");
					String house_area=tokenizerLine.nextToken();//区域部分
					String house_type=tokenizerLine.nextToken();//类型部分
					String house_month=tokenizerLine.nextToken();//月份部分
					String house_totalprice=tokenizerLine.nextToken();//总价部分
					if (house_totalprice.equals("-1.0")) {
						continue;
					}
					Text name=new Text(house_area+" "+house_type+" "+house_month);
					context.write(name, new Text(house_totalprice));
				}
	}

	

}
