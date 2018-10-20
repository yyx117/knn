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
	//��̬�ڲ���
	public static class WCMapper extends
		Mapper<LongWritable,Text,Text,IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException
				  {
			
			//�ÿո���һ�����ݣ���ֳ���ÿ��С�ξ���һ������
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
			
			//����shuffle�Ժ�map���key��ͬ�ļ�ֵ�Ի������ͬһ��reduce������
			//��������ֻ������һ��key����value�γ�һ������
			//key����ĳһ�����ʣ�values����һ��1,ֻ��Ҫ��values����
			int sum=0;
			for (IntWritable value : values) {
				sum += value.get();				
			}
			context.write(key, new IntWritable(sum));
		}
		}
	//��������
	public static void main(String[] args) throws Exception  {
		//1 ������ö���
			Configuration conf = new Configuration();
		//2 ����������
			Job job = Job.getInstance(conf,"wordcount");
			job.setJarByClass(WordCount.class);
		//3  Ϊ����װ��mapper��
			job.setMapperClass(WCMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
		//4 Ϊ����װ��reducer��
			job.setReducerClass(WCReaducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
		//5  ������������·��
			TextInputFormat.addInputPath(job, new Path("src/test.txt"));
		//6  ���ý�����·��
			TextOutputFormat.setOutputPath(job, new Path("src/count_result"));		
		//7 �ύ����
			job.waitForCompletion(true);
	}
}
