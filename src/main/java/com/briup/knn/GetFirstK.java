package com.briup.knn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GetFirstK {
	public static class GFKMapper 
		extends Mapper<LongWritable, Text,
			TagDegree, IntWritable>{
		@Override
		protected void map(LongWritable key, 
				Text value,
				Mapper<LongWritable, Text,
				TagDegree, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] ss = value.toString().split("\t");	
			Text tag = new Text(ss[0]);
			DoubleWritable degree = 
				new DoubleWritable(
					Double.parseDouble(ss[1]));
			TagDegree td = new TagDegree(tag, degree);
			td.setG(new Text("a"));
			context.write(td, new IntWritable(1));
		}
	}
	public static class GFKReducer 
		extends Reducer<TagDegree, IntWritable, 
			Text, Text>{
		@Override
		protected void reduce(TagDegree key, 
				Iterable<IntWritable> values,
				Reducer<TagDegree, IntWritable,
				Text, Text>.Context context) throws IOException, InterruptedException {
			//1 取前K，20
			//2 计算每个标签出现的次数和平均相似度
			int i = 0;
			Iterator<IntWritable> ite =
					values.iterator();
			Map<String, AvgNum> map = 
					new HashMap<>();
			while(i < 20) {
				IntWritable next = ite.next();
				String tag = key.getTag().toString();
				DoubleWritable degree = key.getDegree();
				if(!map.containsKey(tag)) {
					AvgNum an = 
						new AvgNum(degree, new IntWritable(1));
					map.put(tag, an);
				}else {
					AvgNum an = map.get(tag);
					DoubleWritable old_avg = an.getAvg();
					IntWritable old_num = an.getNum();
					IntWritable new_num = 
						new IntWritable(old_num.get()+1);
					double new_a = (old_avg.get()*old_num.get()
								+degree.get())
								/new_num.get();      
					DoubleWritable new_avg = 
						new DoubleWritable(new_a);
					AvgNum new_an = new AvgNum(new_avg,new_num);
					map.put(tag, new_an);
				}
				i++;
			}//while
			
			for (Map.Entry<String, AvgNum> en 
					: map.entrySet()) {
				Text tag = new Text(en.getKey());
				Text v = new Text(en.getValue().toString());
				System.out.println(tag+"\t"+v+"------");
				context.write(tag, v);
			}
			
		}
	}
	public static void main(String[] args)throws Exception {
		//1 获得配置对象
		Configuration conf = new Configuration();
		//2 获得任务对象
		Job job = Job.getInstance(conf,"GetFirstK");
		job.setJarByClass
			(GetFirstK.class);
		//3 为任务装配mapper类
		job.setMapperClass(GFKMapper.class);
		job.setMapOutputKeyClass
			(TagDegree.class);
		job.setMapOutputValueClass
			(IntWritable.class);
		//4 为任务装配reducer类
		job.setReducerClass(GFKReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//5 配置数据输入路径
		TextInputFormat.addInputPath
			(job, new Path("src/name_sd_sorted"));
		//6 配置结果输出路径
		TextOutputFormat.setOutputPath
			(job, new Path("src/gfk_res"));
		job.setGroupingComparatorClass
			(GFKGroupComparator.class);
		//7 提交任务
		job.waitForCompletion(true);
	}
}

class AvgNum implements Writable{
	private DoubleWritable avg = 
			new DoubleWritable();
	private IntWritable num =
			new IntWritable();
	public AvgNum() {
	}
	public AvgNum(DoubleWritable avg,IntWritable num) {
		this.avg = new DoubleWritable(avg.get());
		this.num = new IntWritable(num.get());
	}
	@Override
	public void write(DataOutput out) throws IOException {
		avg.write(out);
		num.write(out);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		avg.readFields(in);
		num.readFields(in);
	}
	public DoubleWritable getAvg() {
		return avg;
	}
	public void setAvg(DoubleWritable avg) {
		this.avg = new DoubleWritable(avg.get());
	}
	public IntWritable getNum() {
		return num;
	}
	public void setNum(IntWritable num) {
		this.num = new IntWritable(num.get());
	}
	
	@Override
	public String toString() {
		return avg.get()+"\t"+num.get();
	}
}







