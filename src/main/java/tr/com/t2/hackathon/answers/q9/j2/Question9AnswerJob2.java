/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q9.j2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import tr.com.t2.hackathon.answers.Answers.BaseAnswerJob;
import tr.com.t2.hackathon.answers.io.LineInputFormat;

/**
 * @author Serkan OZAL
 */
public class Question9AnswerJob2 extends BaseAnswerJob {

    @Override
    public void doJob(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath) {
        job.setJobName("Question 9 [2]");

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
         
        job.setMapperClass(Question9Mapper2.class);
        job.setReducerClass(Question9Reducer2.class);

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
            Long maxConnectionUserId = null;
            Long maxConnection = null;
            // Find global maximum score with its owner user
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("[\\s]+");
                Long userId = Long.parseLong(parts[0]);
                Long connectionCount = Long.parseLong(parts[1]);
                if (maxConnection == null) {
                    maxConnection = connectionCount;
                    maxConnectionUserId = userId;
                }
                else if(connectionCount > maxConnection) {
                    maxConnection = connectionCount;
                    maxConnectionUserId = userId;
                }
                else if(connectionCount == maxConnection && userId > maxConnectionUserId) {
                    maxConnection = connectionCount;
                    maxConnectionUserId = userId;
                }
            }
            is.close();
            
            // Delete old result file
            outputFS.delete(outputFilePath, true);
            
            OutputStream os = outputFS.create(outputFilePath);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
            
            // Write result 
            bw.write(maxConnectionUserId + " " + maxConnection);
                        
            bw.flush();
            bw.close();

            //////////////////////////////////////////////////////////////////
            
            // Find parent path that is actual output path
            String outputPathStr = outputPath.toUri().toString();
            int pathSeparator = outputPathStr.lastIndexOf("/");
            String parentPathStr = outputPathStr.substring(0, pathSeparator);
            
            //////////////////////////////////////////////////////////////////
            
            Path parentPath = new Path(parentPathStr);
            FileStatus[] fileStatusList = outputFS.listStatus(parentPath);
            // Delete all files in parent path, because its copy exists under "${output_path}/1/" directory created at first job
            if (fileStatusList != null) {
                for (FileStatus fileStatus : fileStatusList) {
                    Path filePath = fileStatus.getPath();
                    if (outputFS.isFile(filePath)) {
                        outputFS.delete(filePath, false);
                    }   
                }
            }   
            
            //////////////////////////////////////////////////////////////////
            
            Path parentResultPath = new Path(parentPathStr + "/" + outputFileName);
            
            // Copy final result file to parent path that is actual output path
            FileUtil.copy(
                    outputFilePath.getFileSystem(conf), outputFilePath, 
                    parentResultPath.getFileSystem(conf), parentResultPath, 
                    false, true, conf);
        }
        catch (Throwable t) {
            logger.error("Error occured while processing result", t);
        }
    }

}
