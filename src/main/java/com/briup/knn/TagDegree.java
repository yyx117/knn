package com.briup.knn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TagDegree implements 
	WritableComparable<TagDegree>{
	private Text tag = new Text();
	private DoubleWritable degree = 
			new DoubleWritable();
	//仅用来进行分组
	//只要该属性值相同，不管tag不管degree就分到同一组
	private Text g = new Text();
	
	public TagDegree() {
	}
	public TagDegree(Text tag,DoubleWritable degree) {
		this.tag = new Text(tag.toString());
		this.degree = new DoubleWritable
				(degree.get());
	}
	@Override
	public void write(DataOutput out) throws IOException {
		tag.write(out);
		degree.write(out);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		tag.readFields(in);
		degree.readFields(in);
	}
	@Override
	public int compareTo(TagDegree o) {
		return o.getDegree().
				compareTo(this.degree);
	}
	public Text getTag() {
		return tag;
	}
	public void setTag(Text tag) {
		this.tag = new Text
				(tag.toString());
	}
	public DoubleWritable getDegree() {
		return degree;
	}
	public void setDegree(DoubleWritable degree) {
		this.degree = new DoubleWritable
				(degree.get());
	}
	public Text getG() {
		return g;
	}
	public void setG(Text g) {
		this.g = new Text(g.toString());
	}
}







