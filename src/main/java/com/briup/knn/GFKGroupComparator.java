package com.briup.knn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GFKGroupComparator 
	extends WritableComparator{
	public GFKGroupComparator() {
		super(TagDegree.class,true);
	}
	@Override
	public int compare
		(WritableComparable a,
				WritableComparable b) {
		TagDegree a1 = (TagDegree)a;
		TagDegree b1 = (TagDegree)b;
		
		return a1.getG().compareTo(b1.getG());
	}
}




