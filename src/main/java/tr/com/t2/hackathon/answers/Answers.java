/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import tr.com.t2.hackathon.answers.q1.Question1AnswerJob;
import tr.com.t2.hackathon.answers.q2.Question2AnswerJob;
import tr.com.t2.hackathon.answers.q3.Question3AnswerJob;
import tr.com.t2.hackathon.answers.q4.Question4AnswerJob;
import tr.com.t2.hackathon.answers.q5.Question5AnswerJob;
import tr.com.t2.hackathon.answers.q6.Question6AnswerJob;
import tr.com.t2.hackathon.answers.q7.Question7AnswerJob;
import tr.com.t2.hackathon.answers.q8.Question8AnswerJob;
import tr.com.t2.hackathon.answers.q9.Question9AnswerJob;

/**
 * @author Serkan OZAL
 */
public class Answers {

    private static final Logger logger = Logger.getLogger(Answers.class);
    
    public static final String HACKATHON_INPUT_BUCKET = "s3n://t2-hackathon-sampledata";
    public static final String HACKATHON_OUTPUT_BUCKET = "s3n://t2-hackathon-answers";
    
    public static final String KEY_VALUE_SEPARATOR = " ";
    
    enum MemoryType {
        
        LOW,
        MEDIUM,
        HIGH
        
    }

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        long start, finish;
        long executionTimeInSeconds, executionTimeInMinutes;
        MemoryType memoryType = null;
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        int questionNo = Integer.parseInt(args[2]);
        
        if (args.length > 3) {
            for (int i = 3; i < args.length; i++) {
                if (args[i].equalsIgnoreCase("-lm")) {
                    memoryType = MemoryType.LOW;
                    break;
                }
                else if (args[i].equalsIgnoreCase("-mm")) {
                    memoryType = MemoryType.MEDIUM;
                    break;
                }
                else if (args[i].equalsIgnoreCase("-hm")) {
                    memoryType = MemoryType.HIGH;
                    break;
                }
            }
        }
        
        // Do memory type specific configurations
        JobConf conf = createDefaultJobConfiguration(memoryType);

        ////////////////////////////////////////////////////////////////////////////////////
                
        FileSystem outputFS = outputPath.getFileSystem(conf);
        while (true) {
            if (outputFS.exists(outputPath)) {
                outputFS.delete(outputPath, true);
                logger.info("Deleting existing output path: " + outputPath.toUri().toString());
                Thread.sleep(1000); 
            }
            else {
                break;
            }
        }
        
        ////////////////////////////////////////////////////////////////////////////////////
        
        AnswerJob answerJob = createAnswerJob(questionNo);
        
        ////////////////////////////////////////////////////////////////////////////////////
        
        answerJob.doConfig(args, conf, inputPath, outputPath);

        ////////////////////////////////////////////////////////////////////////////////////
        
        Job job = new Job(conf);
        
        job.setJarByClass(Answers.class);
         
        ////////////////////////////////////////////////////////////////////////////////////
        
        // If input path is file, use only that files
        FileSystem inputFS = inputPath.getFileSystem(conf);
        if (inputFS.isFile(inputPath)) {
            FileInputFormat.addInputPath(job, inputPath);
        }
        // Else, use all "txt" files under input directory
        else {
            FileStatus[] fileStatusList = inputFS.listStatus(inputPath);
            if (fileStatusList != null) {
                for (FileStatus fileStatus : fileStatusList) {
                    Path filePath = fileStatus.getPath();
                    if (inputFS.isFile(filePath) && filePath.toUri().toString().endsWith(".txt")) {
                        FileInputFormat.addInputPath(job, filePath);
                    }   
                }
            }   
        }
        
        FileOutputFormat.setOutputPath(job, outputPath);
    
        answerJob.doJob(args, job, conf, inputPath, outputPath);

        ////////////////////////////////////////////////////////////////////////////////////
        
        logger.info("MapReduce job started ...");
        start = System.currentTimeMillis();
        
        // Start job and wait it to finish
        job.waitForCompletion(true);
        
        finish = System.currentTimeMillis();
        executionTimeInSeconds = (finish - start) / 1000;
        executionTimeInMinutes = executionTimeInSeconds / 60;
        logger.info("MapReduce job finished in " + 
                        executionTimeInSeconds + " seconds " + 
                        "(" + executionTimeInMinutes + " minutes" + ")");
        
        ////////////////////////////////////////////////////////////////////////////////////
        
        Properties props = System.getProperties();
        String outputFileName = props.getProperty("outputFileName");
        if (StringUtils.isEmpty(outputFileName)) {
            outputFileName = "output.txt";
        }
        Path resultPath = new Path(args[1] + "/" + outputFileName);
        
        logger.info("MapReduce output merging started ...");
        start = System.currentTimeMillis();
        
        // Merge all partial result files of reducers and generate complete result file
        FileUtil.copyMerge(outputFS, outputPath, outputFS, resultPath, false, conf, null);
        
        answerJob.processResult(args, job, conf, inputPath, outputPath, outputFileName);

        finish = System.currentTimeMillis();
        executionTimeInSeconds = (finish - start) / 1000;
        executionTimeInMinutes = executionTimeInSeconds / 60;
        logger.info("MapReduce output merging finished in " + 
                        executionTimeInSeconds + " seconds " + 
                        "(" + executionTimeInMinutes + " minutes" + ")");
    }
    
    public static JobConf createDefaultJobConfiguration() {
        return createDefaultJobConfiguration(null);
    }
    
    public static JobConf createDefaultJobConfiguration(MemoryType memoryType) {
        JobConf conf = new JobConf();
        
        conf.set("mapred.textoutputformat.separator", KEY_VALUE_SEPARATOR); // Prior to Hadoop 2 (YARN)
        conf.set("mapreduce.textoutputformat.separator", KEY_VALUE_SEPARATOR);  // Hadoop v2+ (YARN)
        conf.set("mapreduce.output.textoutputformat.separator", KEY_VALUE_SEPARATOR);
        conf.set("mapreduce.output.key.field.separator", KEY_VALUE_SEPARATOR);
        conf.set("mapred.textoutputformat.separatorText", KEY_VALUE_SEPARATOR); 
        
        // Set serializers
        //      * WritableSerialization for writables suchas IntWritable, Text, ...
        //      * JavaSerialization for all remaining objects
        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
        
        conf.setLong("mapred.task.timeout", 100 * 60 * 1000);
        
        if (memoryType != null) {
            switch (memoryType) {
                case LOW:
                    conf.set("mapred.child.java.opts", "-Xms512m -Xmx512m -XX:-UseGCOverheadLimit");
                    conf.set("mapreduce.map.java.opts", "-Xms512m -Xmx512m -XX:-UseGCOverheadLimit");
                    conf.setInt("mapreduce.map.memory.mb", 512);
                    conf.set("mapreduce.reduce.java.opts", "-Xms512m -Xmx512m -XX:-UseGCOverheadLimit");
                    conf.setInt("mapreduce.reduce.memory.mb", 512);
                    conf.setInt("yarn.app.mapreduce.am.resource.mb", 512);
                    conf.set("yarn.app.mapreduce.am.command-opts", "-Xms512m -Xmx512m -XX:-UseGCOverheadLimit");
                    break;
                    
                case MEDIUM:
                    conf.set("mapred.child.java.opts", "-Xms1024m -Xmx1536m -XX:-UseGCOverheadLimit");
                    conf.set("mapreduce.map.java.opts", "-Xms1024m -Xmx1536m -XX:-UseGCOverheadLimit");
                    conf.setInt("mapreduce.map.memory.mb", 3 * 512);
                    conf.set("mapreduce.reduce.java.opts", "-Xms1024m -Xmx1536m -XX:-UseGCOverheadLimit");
                    conf.setInt("mapreduce.reduce.memory.mb", 3 * 512);
                    conf.setInt("yarn.app.mapreduce.am.resource.mb", 3 * 512);
                    conf.set("yarn.app.mapreduce.am.command-opts", "-Xms1024m -Xmx1536m -XX:-UseGCOverheadLimit");
                    break;
                    
                case HIGH:
                    conf.set("mapred.child.java.opts", "-Xms4g -Xmx6g -XX:-UseGCOverheadLimit");
                    conf.set("mapreduce.map.java.opts", "-Xms4g -Xmx6g -XX:-UseGCOverheadLimit");
                    conf.setInt("mapreduce.map.memory.mb", 6 * 1024);
                    conf.set("mapreduce.reduce.java.opts", "-Xms4g -Xmx6g -XX:-UseGCOverheadLimit");
                    conf.setInt("mapreduce.reduce.memory.mb", 6 * 1024);
                    conf.setInt("yarn.app.mapreduce.am.resource.mb", 6 * 1024);
                    conf.set("yarn.app.mapreduce.am.command-opts", "-Xms4g -Xmx6g -XX:-UseGCOverheadLimit");
                    break;
            }
        }
        
        return conf;
    }
    
    public interface AnswerJob {
        
        void doConfig(String[] args, JobConf conf, Path inputPath, Path outputPath);
        void doJob(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath);
        void processResult(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath, String outputFileName);
        
    }
    
    public static abstract class BaseAnswerJob implements AnswerJob {
        
        protected final Logger logger = Logger.getLogger(getClass());
        
        @Override
        public void doConfig(String[] args, JobConf conf, Path inputPath, Path outputPath) {
            
        }
        
        @Override
        public void doJob(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath) {
            
        }
        
        @Override
        public void processResult(String[] args, Job job, JobConf conf, Path inputPath, Path outputPath, String outputFileName) {
            
        }
        
    }
    
    private static AnswerJob createAnswerJob(int questionNo) {
        switch (questionNo) {
            case 1:
                return new Question1AnswerJob();
            case 2:
                return new Question2AnswerJob();
            case 3:
                return new Question3AnswerJob();
            case 4:
                return new Question4AnswerJob();
            case 5:
                return new Question5AnswerJob();
            case 6:
                return new Question6AnswerJob();
            case 7:
                return new Question7AnswerJob();
            case 8:
                return new Question8AnswerJob();
            case 9:
                return new Question9AnswerJob();
            default:
                throw new IllegalArgumentException("Unknown question number " + questionNo);
        }
    }
    
    public static abstract class BaseMapper<K1, V1, K2, V2> extends Mapper<K1, V1, K2, V2> {
        
        protected final static IntWritable ONE = new IntWritable(1);
        
        protected final Logger logger = Logger.getLogger(getClass()); 

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            init(context);
            super.run(context);
        }

        protected void init(Context context) {
            try {
                logger.info("Mapper has been initialized ...");
            }
            catch (Throwable t) {
                logger.error("Error occured while initializing Mapper", t);
            }
        }
        
    }
    
    public static abstract class BaseReducer<K1, V1, K2, V2> extends Reducer<K1, V1, K2, V2> {
        
        protected final static IntWritable ONE = new IntWritable(1);
        
        protected final Logger logger = Logger.getLogger(getClass()); 
        
        @Override
        public void run(Context context) throws IOException, InterruptedException {
            init(context);
            super.run(context);
        }

        protected void init(Context context) {
            try {
                logger.info("Reducer has been initialized ...");
            }
            catch (Throwable t) {
                logger.error("Error occured while initializing Reducer", t);
            }
        }
        
    }

}
