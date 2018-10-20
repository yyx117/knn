package com.briup.knn;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class GetSimilarityDegree {
	static char[] array = new char[400];
	public static class GSDMapper 
		extends Mapper<LongWritable, Text, 
			Text, DoubleWritable>{
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String name = 
					new File("src/x_bin")
					.list()[0];
			FileReader in = new FileReader("src/x_bin/"+name);
			in.read(array);
			in.close();
		}
		@Override
		protected void map(LongWritable key, 
			Text value,
				Mapper<LongWritable, Text, 
					Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			//1 文件名
			FileSplit is = (FileSplit)
				context.getInputSplit();
			String name = is.getPath().getName();
			//2 相似度
			char[] train_array = 
				value.toString().
					toCharArray();
			double sum = 0;
			for(int i = 0;i < 400;i++) {
				int x = Integer.parseInt(
					Character.toString
						(array[i]));
				int t = Integer.parseInt(
					Character.toString
						(train_array[i]));
				sum += (x-t)*(x-t);
			}
			//欧氏距离
			double ed = Math.sqrt(sum);
			//相似度
			double degree = 1/(ed+1);
			//3 输出结果
			context.write
				(new Text(name), 
					new DoubleWritable(degree));
		}
	}
	public static void main(String[] args)
			throws Exception {
		//1 获得配置对象
		Configuration conf = new Configuration();
		//2 获得任务对象
		Job job = Job.getInstance(conf,"getSD");
		job.setJarByClass
			(GetSimilarityDegree.class);
		//3 为任务装配mapper类
		job.setMapperClass(GSDMapper.class);
		job.setMapOutputKeyClass
			(Text.class);
		job.setMapOutputValueClass
			(DoubleWritable.class);
		//4 为任务装配reducer类
//		job.setReducerClass(WCReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
		//5 配置数据输入路径
		TextInputFormat.addInputPath
			(job, new Path("src/train_bin"));
		//6 配置结果输出路径
		TextOutputFormat.setOutputPath
			(job, new Path("src/name_sd"));
		//7 提交任务
		job.waitForCompletion(true);
	
	}
}







