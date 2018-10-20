package com.briup.knn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import javax.servlet.jsp.tagext.Tag;

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

import com.briup.knn.GetFirstK.GFKMapper;
import com.briup.knn.GetFirstK.GFKReducer;

public class GetLastResult {
	public static class GLRMapper extends 
		Mapper<LongWritable, Text, 
			Text, TagAvgNum>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TagAvgNum>.Context context)
				throws IOException, InterruptedException {
			String[] ss = value.toString().split("\t");
			Text tag = new Text(ss[0]);
			DoubleWritable avg = 
				new DoubleWritable
					(Double.parseDouble(ss[1]));
			IntWritable num = new IntWritable
				(Integer.parseInt(ss[2]));
			TagAvgNum tan = new TagAvgNum(tag, avg, num);
			context.write(new Text("a"), tan);
		}
	}
	public static class GLRReducer
		extends Reducer<Text, TagAvgNum, 
		Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<TagAvgNum> values,
				Reducer<Text, TagAvgNum, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			Iterator<TagAvgNum> ite = values.iterator();
			TagAvgNum max = new TagAvgNum(ite.next());
			while (ite.hasNext()) {
				TagAvgNum c_tan = new TagAvgNum(ite.next());
				if(c_tan.getNum().get() > max.getNum().get()) {
					max = new TagAvgNum(c_tan);
				}else if(c_tan.getNum().get() 
						== max.getNum().get()){
					if(c_tan.getAvg().get() > max.getAvg().get()) {
						max = new TagAvgNum(c_tan);
					}
				}
			}
			context.write(max.getTag(),
					NullWritable.get());
		}
	}
	public static void main(String[] args)
		throws Exception{

		//1 获得配置对象
		Configuration conf = new Configuration();
		//2 获得任务对象
		Job job = Job.getInstance(conf,"GetLastResult");
		job.setJarByClass
			(GetLastResult.class);
		//3 为任务装配mapper类
		job.setMapperClass(GLRMapper.class);
		job.setMapOutputKeyClass
			(Text.class);
		job.setMapOutputValueClass
			(TagAvgNum.class);
		//4 为任务装配reducer类
		job.setReducerClass(GLRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//5 配置数据输入路径
		TextInputFormat.addInputPath
			(job, new Path("src/gfk_res"));
		//6 配置结果输出路径
		TextOutputFormat.setOutputPath
			(job, new Path("src/last_res"));
		//7 提交任务
		job.waitForCompletion(true);
	
	}
	
}

class TagAvgNum implements Writable{
	private Text tag = new Text();
	private DoubleWritable avg = new DoubleWritable();
	private IntWritable num = new IntWritable();
	public TagAvgNum() {
	}
	public TagAvgNum(Text tag,
			DoubleWritable avg, 
			IntWritable num) {
		this.tag = new Text(tag.toString());
		this.avg = new DoubleWritable(avg.get());
		this.num = new IntWritable(num.get());
	}
	public TagAvgNum(TagAvgNum tan) {
		this.tag = new Text(tan.tag.toString());
		this.avg = new DoubleWritable(tan.avg.get());
		this.num = new IntWritable(tan.num.get());
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		tag.write(out);
		avg.write(out);
		num.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tag.readFields(in);
		avg.readFields(in);
		num.readFields(in);
	}

	public Text getTag() {
		return tag;
	}

	public void setTag(Text tag) {
		this.tag = new Text(tag.toString());
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
	
	
}
