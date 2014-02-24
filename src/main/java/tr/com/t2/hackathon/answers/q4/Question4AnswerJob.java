/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q4;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import tr.com.t2.hackathon.answers.Answers.BaseAnswerJob;
import tr.com.t2.hackathon.answers.io.LineInputFormat;

/**
 * @author Serkan OZAL
 */
public class Question4AnswerJob extends BaseAnswerJob {

	public static final String LANGUAGE_PARAMETER_NAME = "lang";
	public static final int LANGUAGE_PARAMETER_ORDER = 3;
	
	@Override
	public void doConfig(String[] args, JobConf conf, Path inputPath, Path outputPath) {
		if (args.length >= LANGUAGE_PARAMETER_ORDER + 1) {
			// Set language parameter if specified
			conf.set(LANGUAGE_PARAMETER_NAME, args[LANGUAGE_PARAMETER_ORDER]); // Set 4. argument as language parameter
			logger.info("Language parameter set with name: " + LANGUAGE_PARAMETER_NAME + 
						" and value: " + args[LANGUAGE_PARAMETER_ORDER]);
		}	
	}
	
	@Override
	public void doJob(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath) {
		job.setJobName("Question 4");
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		 
		job.setMapperClass(Question4Mapper.class);
		job.setReducerClass(Question4Reducer.class);

		job.setInputFormatClass(LineInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	}
	
	@Override
	public void processResult(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath, String outputFileName) {
		try {
			FileSystem outputFS = outputPath.getFileSystem(conf);
			Path outputFilePath = new Path(outputPath + "/" + outputFileName);
			
			InputStream is = outputFS.open(outputFilePath);
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String line;
			int totalCount = 0;
			// Calculate sum of counts
			while ((line = br.readLine()) != null) {
				Integer count = Integer.parseInt(line);
				totalCount += count;
			}
			is.close();
			
			// Delete old result file
			outputFS.delete(outputFilePath, true);
			
			OutputStream os = outputFS.create(outputFilePath);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
			
			// Write result 
			bw.write(String.valueOf(totalCount));
			
			bw.flush();
			bw.close();
		}
		catch (Throwable t) {
			logger.error("Error occured while processing result", t);
		}
	}
	
}
