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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import tr.com.t2.hackathon.answers.Answers.BaseAnswerJob;

/**
 * @author Serkan OZAL
 */
public class Question2AnswerJob extends BaseAnswerJob {

    @Override
    public void doJob(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath) {
        job.setJobName("Question 2");
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
         
        job.setMapperClass(Question2Mapper.class);
        job.setReducerClass(Question2Reducer.class);

        job.setInputFormatClass(Question2InputFormat.class);
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
            Set<GroupIdAndAmountTupple> result = new TreeSet<GroupIdAndAmountTupple>();
            // Read merged but unsorted result file and save to memory
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("[\\s]+");
                Long groupId = Long.parseLong(parts[0]);
                Long amount = Long.parseLong(parts[1]);
                result.add(new GroupIdAndAmountTupple(groupId, amount));
            }
            is.close();
            
            // Delete old result file
            outputFS.delete(outputFilePath, true);
            
            // Write new sorted result file
            OutputStream os = outputFS.create(outputFilePath);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
            for (GroupIdAndAmountTupple r : result) {
                bw.write(r.groupId + " " + r.amount + "\n");
            }
            bw.flush();
            bw.close();
        }
        catch (Throwable t) {
            logger.error("Error occured while processing result", t);
        }
    }
    
    class GroupIdAndAmountTupple implements Comparable<GroupIdAndAmountTupple> {
        
        Long groupId;
        Long amount;
        
        GroupIdAndAmountTupple(Long groupId, Long amount) {
            this.groupId = groupId;
            this.amount = amount;
        }
        
        @Override
        public int compareTo(GroupIdAndAmountTupple o) {
            return groupId.compareTo(o.groupId);
        }
        
    }

}
