package com.briup.knn;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.briup.knn.GetFirstK.GFKMapper;
import com.briup.knn.GetFirstK.GFKReducer;
import com.briup.knn.GetLastResult.GLRMapper;
import com.briup.knn.GetLastResult.GLRReducer;
import com.briup.knn.GetSimilarityDegree.GSDMapper;
import com.briup.knn.SortedByDegree.SBDMapper;
import com.briup.knn.SortedByDegree.SBDReducer;

public class HandWritingRecognition {
	public static void main(String[] args) throws Exception {
		picToBin();
		System.out.println("待识别图片二值化完成");
		
		Configuration conf = new Configuration();
		Job getSDJob = Job.getInstance(conf, "getSDJob");
		getSDJob.setJarByClass(GetSimilarityDegree.class);
		// 3 为任务装配mapper类
		getSDJob.setMapperClass(GSDMapper.class);
		getSDJob.setMapOutputKeyClass(Text.class);
		getSDJob.setMapOutputValueClass(DoubleWritable.class);
		// 5 配置数据输入路径
		TextInputFormat.addInputPath(getSDJob, new Path("src/train_bin"));
		// 6 配置结果输出路径
		TextOutputFormat.setOutputPath(getSDJob, new Path("src/name_sd"));

		Job sortBySDJob = Job.getInstance(conf, "sortBySDJob");
		sortBySDJob.setJarByClass(SortedByDegree.class);
		// 3 为任务装配mapper类
		sortBySDJob.setMapperClass(SBDMapper.class);
		sortBySDJob.setMapOutputKeyClass(TagDegree.class);
		sortBySDJob.setMapOutputValueClass(NullWritable.class);
		// 4 为任务装配reducer类
		sortBySDJob.setReducerClass(SBDReducer.class);
		sortBySDJob.setOutputKeyClass(Text.class);
		sortBySDJob.setOutputValueClass(DoubleWritable.class);
		// 5 配置数据输入路径
		TextInputFormat.addInputPath(sortBySDJob, new Path("src/name_sd"));
		// 6 配置结果输出路径
		TextOutputFormat.setOutputPath(sortBySDJob, new Path("src/name_sd_sorted"));

		Job getFKJob = Job.getInstance(conf, "getFKJob");
		getFKJob.setJarByClass(GetFirstK.class);
		// 3 为任务装配mapper类
		getFKJob.setMapperClass(GFKMapper.class);
		getFKJob.setMapOutputKeyClass(TagDegree.class);
		getFKJob.setMapOutputValueClass(IntWritable.class);
		// 4 为任务装配reducer类
		getFKJob.setReducerClass(GFKReducer.class);
		getFKJob.setOutputKeyClass(Text.class);
		getFKJob.setOutputValueClass(Text.class);
		// 5 配置数据输入路径
		TextInputFormat.addInputPath(getFKJob, new Path("src/name_sd_sorted"));
		// 6 配置结果输出路径
		TextOutputFormat.setOutputPath(getFKJob, new Path("src/gfk_res"));
		getFKJob.setGroupingComparatorClass(GFKGroupComparator.class);

		Job getLRJob = Job.getInstance(conf, "getLRJob");
		getLRJob.setJarByClass(GetLastResult.class);
		// 3 为任务装配mapper类
		getLRJob.setMapperClass(GLRMapper.class);
		getLRJob.setMapOutputKeyClass(Text.class);
		getLRJob.setMapOutputValueClass(TagAvgNum.class);
		// 4 为任务装配reducer类
		getLRJob.setReducerClass(GLRReducer.class);
		getLRJob.setOutputKeyClass(Text.class);
		getLRJob.setOutputValueClass(NullWritable.class);
		// 5 配置数据输入路径
		TextInputFormat.addInputPath(getLRJob, new Path("src/gfk_res"));
		// 6 配置结果输出路径
		TextOutputFormat.setOutputPath(getLRJob, new Path("src/last_res"));

		ControlledJob getSD = new ControlledJob(getSDJob.getConfiguration());
		ControlledJob sortBySD = new ControlledJob(sortBySDJob.getConfiguration());
		ControlledJob getFK = new ControlledJob(getFKJob.getConfiguration());
		ControlledJob getLR = new ControlledJob(getLRJob.getConfiguration());
		getLR.addDependingJob(getFK);
		getFK.addDependingJob(sortBySD);
		sortBySD.addDependingJob(getSD);

		JobControl con = new JobControl("HandWritingRecognition");
		con.addJob(getSD);
		con.addJob(sortBySD);
		con.addJob(getFK);
		con.addJob(getLR);

		Thread t = new Thread(con);
		t.start();

		while (true) {
			if (con.allFinished()) {
				System.out.println("图片识别完毕，请查看结果");
				System.exit(0);
			}
		}
	}
	public static void picToBin() throws Exception{
		String name = 
			new File("src/x").list()[0];
		BufferedImage img_x = 
			ImageIO.read(
				new File("src/x/"+name));
		int width = img_x.getWidth();
		int height = img_x.getHeight();
		FileWriter out = 
			new FileWriter("src/x_bin/"+
				name.substring(0, 
					name.indexOf(".")));
		
		for(int j = 0;j < height;j++) {
			//从左到右遍历一行像素点
			for(int i = 0;i < width;i++) {
				//拿到每个像素点色颜色
				int rgb_x = img_x.getRGB(i, j);
				Color gray = 
						new Color(105, 105, 105);
				int rgb_gray = gray.getRGB();
				// 如果颜色超过灰色 转化该点为字符"1"
				if(rgb_x > rgb_gray) {
					System.out.print("1");
					out.write("1");
					out.flush();
				}
				// 如果颜色比灰色浅 转化该点为字符"0"
				else {
					System.out.print("0");
					out.write("0");
					out.flush();
				}
			}//内层for
		}//外层for
		out.close();
	
	}
	
	
}
