/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q9;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import tr.com.t2.hackathon.answers.Answers.BaseAnswerJob;
import tr.com.t2.hackathon.answers.q9.j1.Question9AnswerJob1;

/**
 * @author Serkan OZAL
 */
/*
 * *** NOTE ***
 *  
 * Since "reduce" method of Combiner is called when values of a key reaches a limit, 
 * "values" argument possibly doesn't contain all values of specified key. 
 * In addition, there is no guarantee that all same keys go go to same combiner as reducer. 
 * So, using combiner is not suitable for this job and we use two sequential job to solve problem.
 * First job finds first level connections.
 * Second job finds second level connections and by adding them to first level connections, finds final result
 */
public class Question9AnswerJob extends BaseAnswerJob {

	private Question9AnswerJob1 job1 = new Question9AnswerJob1();
	
	@Override
	public void doJob(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath) {
		job1.doJob(args, job, conf, inputPath, outputPath);
	}
	
	@Override
	public void processResult(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath, String outputFileName) {
		job1.processResult(args, job, conf, inputPath, outputPath, outputFileName);
	}
	
}
