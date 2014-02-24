/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Serkan OZAL
 */
public class Question5Partitioner extends Partitioner<Text, LongWritable> {

	@Override
	public int getPartition(Text key, LongWritable value, int numPartitions) {
		// If emitted value is specified for minimum id calculation, forward it to 0. partition (reducer)
		if (key.toString().equalsIgnoreCase(Question5AnswerJob.MIN_ID_KEY.toString())) {
			return 0; // Write min id first in output
		}
		// If emitted value is specified for maximum id calculation, forward it to 1. partition (reducer)
		else if (key.toString().equalsIgnoreCase(Question5AnswerJob.MAX_ID_KEY.toString())) {
			return 1; // Write min id last in output
		}
		// Else forward it to 2. partition (reducer)
		else {
			return 2;
		}
	} 
   
}
