/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import tr.com.t2.hackathon.answers.Answers.BaseAnswerJob;
import tr.com.t2.hackathon.answers.io.LineInputFormat;

/**
 * @author Serkan OZAL
 */
public class Question1AnswerJob extends BaseAnswerJob {

	@Override
	public void doJob(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath) {
		job.setJobName("Question 1");
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		 
		job.setMapperClass(Question1Mapper.class);
		job.setReducerClass(Question1Reducer.class);

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
			// Keep result as sorted with their key
			Set<CountryCodeAndFreqTupple> result = new TreeSet<CountryCodeAndFreqTupple>();
			// Read merged but unsorted result file and save to memory
			while ((line = br.readLine()) != null) {
				String[] parts = line.split("[\\s]+");
				String countryCode = parts[0];
				String countryFreq = parts[1];
				result.add(new CountryCodeAndFreqTupple(countryCode, Integer.parseInt(countryFreq)));
			}
			is.close();
			
			// Delete old result file
			outputFS.delete(outputFilePath, true);
			
			OutputStream os = outputFS.create(outputFilePath);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
			// Write new sorted result file
			for (CountryCodeAndFreqTupple r : result) {
				bw.write(r.countryCode + " " + r.countryFreq + "\n");
			}
			bw.flush();
			bw.close();
		}
		catch (Throwable t) {
			logger.error("Error occured while processing result", t);
		}
	}
	
	class CountryCodeAndFreqTupple implements Comparable<CountryCodeAndFreqTupple> {
		
		String countryCode;
		Integer countryFreq;
		
		CountryCodeAndFreqTupple(String countryCode, Integer countryFreq) {
			this.countryCode = countryCode;
			this.countryFreq = countryFreq;
		}
		
		@Override
		public int compareTo(CountryCodeAndFreqTupple o) {
			return countryCode.compareToIgnoreCase(o.countryCode);
		}
		
	}

}
