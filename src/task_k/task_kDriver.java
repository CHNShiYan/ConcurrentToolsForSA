package task_k;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

public class task_kDriver {
	public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length<2) {
			System.out.println("args less");
			return;
		}
		//获取输入和输出的路径参数
		Path inputPath=new Path(args[0]);
		Path outputPath=new Path(args[1]);
		
		//创建配置，实例化Job
		Configuration conf=new Configuration();
		Job job =Job.getInstance(conf,"task_k");
		
		//设置入口
		job.setJarByClass(task_kDriver.class);
		job.setMapperClass(task_kMapper.class);
		job.setReducerClass(task_kReducer.class);
		
		//设置输出的键值类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
				
		//设置输入和输出的文件格式以及路径
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
				
		//执行Job并等待完成
		job.waitForCompletion(true);
	}

}
