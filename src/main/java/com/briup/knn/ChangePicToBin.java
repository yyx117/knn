package com.briup.knn;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import javax.imageio.ImageIO;

public class ChangePicToBin {
	public static void main(String[] args) 
		throws Exception{
		// 1 代表图片
		// 2 可以获取到每个像素点的颜色
		BufferedImage img_x = 
			ImageIO.read(
				new File("src/x.png"));
		// 遍历图片的每个像素点
		// 对于像素点的颜色进行判断
		int width = img_x.getWidth();
		int height = img_x.getHeight();
		FileWriter out = 
			new FileWriter("src/x_bin");
		
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
	}//main
}//class
