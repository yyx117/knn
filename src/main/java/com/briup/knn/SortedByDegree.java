package com.briup.knn;

import java.io.IOException;

import javax.servlet.jsp.tagext.Tag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.briup.knn.GetSimilarityDegree.GSDMapper;

public class SortedByDegree {
	public static class SBDMapper 
		extends Mapper<LongWritable, Text, 
		TagDegree, NullWritable>{
		@Override
		protected void map(LongWritable key, 
			Text value,
				Mapper<LongWritable, Text, 
				TagDegree, NullWritable>
					.Context context)
				throws IOException, InterruptedException {
			//1 构建复合键值
			String line = value.toString();
			String[] ss = line.split("\t");
			String name = ss[0];
			String tag = name.substring(0, 1);
			double d = Double.parseDouble(ss[1]);
			TagDegree td = new TagDegree
				(new Text(tag),new DoubleWritable(d));
			//2 输出
			context.write(td, NullWritable.get());
		}
	}
	public static class SBDReducer extends 
		Reducer<TagDegree, NullWritable, 
			Text, DoubleWritable>{
		@Override
		protected void reduce(TagDegree key, 
				Iterable<NullWritable> values,
				Reducer<TagDegree, NullWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			for (NullWritable value : values) {
				Text tag = key.getTag();
				DoubleWritable degree = 
						key.getDegree();
				context.write(tag, degree);
			}
		}
	}
	
	public static void main(String[] args)
			throws Exception {
		//1 获得配置对象
		Configuration conf = new Configuration();
		//2 获得任务对象
		Job job = Job.getInstance(conf,"Sort");
		job.setJarByClass
			(SortedByDegree.class);
		//3 为任务装配mapper类
		job.setMapperClass(SBDMapper.class);
		job.setMapOutputKeyClass
			(TagDegree.class);
		job.setMapOutputValueClass
			(NullWritable.class);
		//4 为任务装配reducer类
		job.setReducerClass(SBDReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		//5 配置数据输入路径
		TextInputFormat.addInputPath
			(job, new Path("src/name_sd"));
		//6 配置结果输出路径
		TextOutputFormat.setOutputPath
			(job, new Path("src/name_sd_sorted"));
		//7 提交任务
		job.waitForCompletion(true);
	
	}
}



