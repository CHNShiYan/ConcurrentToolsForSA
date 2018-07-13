package task_ii;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;

public class task_dDriver {
	public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException {
		
		//获取输入和输出的路径参数
		Path inputPath=new Path(args[0]);
		
		//创建配置，实例化Job
		Configuration conf=new Configuration();
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY ,"com.mysql.jdbc.Driver");
		conf.set(DBConfiguration.URL_PROPERTY ,"jdbc:mysql://192.168.20.150:3306/test?useUnicode=true&characterEncoding=utf8");
		conf.set(DBConfiguration.USERNAME_PROPERTY ,"root");
		conf.set(DBConfiguration.PASSWORD_PROPERTY,"123456");
		Job job =Job.getInstance(conf,"task_d");

		//设置入口
		job.setJarByClass(task_dDriver.class);
		job.setMapperClass(task_dMapper.class);
		job.setReducerClass(task_dReducer.class);
	
		//设置输出的键值类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		Path path=new Path("/lib/mysql-connector-java-5.1.46-bin.jar");
		job.addFileToClassPath(path);
		job.setOutputFormatClass(DBOutputFormat.class);
				
		//设置输入和输出的文件格式以及路径
		FileInputFormat.setInputPaths(job, inputPath);
		DBOutputFormat.setOutput(job, "t_avgpmfee", "area","type","average");
				
		//执行Job并等待完成
		boolean flag=job.waitForCompletion(true);
		if(flag)System.exit(0);
		System.exit(1);
	}

}
