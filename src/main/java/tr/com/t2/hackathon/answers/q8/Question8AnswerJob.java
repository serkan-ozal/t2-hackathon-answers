/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q8;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import tr.com.t2.hackathon.answers.Answers.BaseAnswerJob;
import tr.com.t2.hackathon.answers.io.LineInputFormat;

/**
 * @author Serkan OZAL
 */
public class Question8AnswerJob extends BaseAnswerJob {

    @Override
    public void doJob(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath) {
        job.setJobName("Question 8");
        
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);
         
        job.setMapperClass(Question8Mapper.class);
        job.setReducerClass(Question8Reducer.class);

        job.setInputFormatClass(LineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
    }

}
