/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author Serkan OZAL
 */
public class Question2InputFormat extends FileInputFormat<LongWritable, Question2Data> {

	  @Override
	  public RecordReader<LongWritable, Question2Data> createRecordReader(InputSplit split, TaskAttemptContext context) {
		  return new Question2RecordReader();
	  }

	  @Override
	  protected boolean isSplitable(JobContext context, Path file) {
		  final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		  if (null == codec) {
			  return true;
		  }
		  return codec instanceof SplittableCompressionCodec;
	  }
	  
}
