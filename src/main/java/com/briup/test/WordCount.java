package com.briup.test;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordCount {
	//静态内部类
	public static class WCMapper extends
		Mapper<LongWritable,Text,Text,IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException
				  {
			
			//用空格拆分一行内容，拆分出来每个小段就是一个单词
			String line = value.toString();
			String[] ss = line.split(" ");
			for (String s : ss) {
				context.write(new Text(s),new IntWritable(1));
			}
		}
	}
	public static class WCReaducer extends
		Reducer<Text,IntWritable,Text,IntWritable>{
			
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException  {
			
			//经过shuffle以后，map输出key相同的键值对会输出到同一个reduce方法中
			//输入数据只保留了一个key，把value形成一个集合
			//key就是某一个单词，values就是一堆1,只需要对values遍历
			int sum=0;
			for (IntWritable value : values) {
				sum += value.get();				
			}
			context.write(key, new IntWritable(sum));
		}
		}
	//配置任务
	public static void main(String[] args) throws Exception  {
		//1 获得配置对象
			Configuration conf = new Configuration();
		//2 获得任务对象
			Job job = Job.getInstance(conf,"wordcount");
			job.setJarByClass(WordCount.class);
		//3  为任务装配mapper类
			job.setMapperClass(WCMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
		//4 为任务装配reducer类
			job.setReducerClass(WCReaducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
		//5  配置数据输入路径
			TextInputFormat.addInputPath(job, new Path("src/test.txt"));
		//6  配置结果输出路径
			TextOutputFormat.setOutputPath(job, new Path("src/count_result"));		
		//7 提交任务
			job.waitForCompletion(true);
	}
}
