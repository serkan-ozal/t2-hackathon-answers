/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q9.j1;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import tr.com.t2.hackathon.answers.Answers;
import tr.com.t2.hackathon.answers.Answers.BaseAnswerJob;
import tr.com.t2.hackathon.answers.io.LineInputFormat;
import tr.com.t2.hackathon.answers.q9.j2.Question9AnswerJob2;

/**
 * @author Serkan OZAL
 */
public class Question9AnswerJob1 extends BaseAnswerJob {

    @Override
    public void doJob(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath) {
        job.setJobName("Question 9 [1]");
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
         
        job.setMapperClass(Question9Mapper1.class);
        job.setReducerClass(Question9Reducer1.class);

        job.setInputFormatClass(LineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
    }
    
    @Override
    public void processResult(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath, String outputFileName) {
        try {
            // Copy content of current output directory to sub-folder "/1/" under output directory 
            FileSystem fs = outputPath.getFileSystem(conf);
            FileStatus[] fileStatusList = fs.listStatus(outputPath);
            if (fileStatusList != null) {
                String outputDir = outputPath.toUri().toString() + "/1";
                fs.mkdirs(new Path(outputDir));
                for (FileStatus fileStatus : fileStatusList) {
                    Path filePath = fileStatus.getPath();
                    if (fs.isFile(filePath)) {
                        FileUtil.copy(fs, filePath, fs, new Path(outputDir), false, conf);
                    }   
                }
            }   
            
            //////////////////////////////////////////////////////////////
            
            runSecondJob(args, job, conf, inputPath, outputPath);
        }
        catch (Throwable t) {
            logger.error("Error occured while processing result", t);
        }
    }
    
    @SuppressWarnings("deprecation")
    private void runSecondJob(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath) {
        try {
            JobConf newConf = Answers.createDefaultJobConfiguration();
            
            Question9AnswerJob2 answerJob = new Question9AnswerJob2();
            
            answerJob.doConfig(args, conf, inputPath, outputPath);
            
            Job newJob = new Job(newConf);
            
            newJob.setJarByClass(Question9AnswerJob1.class);
            
            // Output path of second path is sub-folder "/2/" under actual output directory
            String newInputPathStr = outputPath.toUri().toString();
            String newOutputPathStr = outputPath.toUri().toString() + "/2";
            
            Path newInputPath = new Path(newInputPathStr);
            Path newOutputPath = new Path(newOutputPathStr);
            
            FileSystem inputFS = newInputPath.getFileSystem(conf);
            FileSystem outputFS = newOutputPath.getFileSystem(conf);
            
            // If input path is file, use only that files
            if (inputFS.isFile(newInputPath)) {
                FileInputFormat.addInputPath(newJob, newInputPath);
            }
            // Else, use all "txt" files under input directory
            else {
                FileStatus[] fileStatusList = inputFS.listStatus(newInputPath);
                if (fileStatusList != null) {
                    for (FileStatus fileStatus : fileStatusList) {
                        Path filePath = fileStatus.getPath();
                        if (inputFS.isFile(filePath) && filePath.toUri().toString().endsWith(".txt")) {
                            FileInputFormat.addInputPath(newJob, filePath);
                        }   
                    }
                }   
            }
            FileOutputFormat.setOutputPath(newJob, newOutputPath);
            
            //////////////////////////////////////////////////////////////
            
            answerJob.doJob(args, newJob, conf, newInputPath, newOutputPath);
            
            //////////////////////////////////////////////////////////////
            
            newJob.waitForCompletion(true);
            
            //////////////////////////////////////////////////////////////
            
            Properties props = System.getProperties();
            String outputFileName = props.getProperty("outputFileName");
            if (StringUtils.isEmpty(outputFileName)) {
                outputFileName = "output.txt";
            }
            Path resultPath = new Path(newOutputPathStr + "/" + outputFileName);
            
            // Merge all partial result files of reducers and generate complete result file
            FileUtil.copyMerge(outputFS, newOutputPath, outputFS, resultPath, false, conf, null);
    
            //////////////////////////////////////////////////////////////
            
            answerJob.processResult(args, newJob, conf, newInputPath, newOutputPath, outputFileName);
        }
        catch (Throwable t) {
            logger.error("Error occured while processing result", t);
        }
    }

}
