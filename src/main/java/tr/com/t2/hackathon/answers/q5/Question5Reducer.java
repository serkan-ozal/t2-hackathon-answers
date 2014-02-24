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

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 * @author Serkan OZAL
 */
public class Question5Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private static final Logger logger = Logger.getLogger(Question5Reducer.class); 
    
    private boolean useMinId;
    private boolean useMaxId;
    private long minId = Long.MAX_VALUE;
    private long maxId = 0;
    
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

    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        try {
            if (key.toString().equalsIgnoreCase(Question5AnswerJob.MIN_ID_KEY.toString())) {
                for (LongWritable value : values) {
                    if (value.get() < minId) {
                        minId = value.get();
                        useMinId = true;
                    }
                }
            }
            else if (key.toString().equalsIgnoreCase(Question5AnswerJob.MAX_ID_KEY.toString())) {
                for (LongWritable value : values) {
                    if (value.get() > maxId) {
                        maxId = value.get();
                        useMaxId = true;
                    }
                }
            }
        }
        catch (Throwable t) {
            logger.error("Error occured while executing reduce function of Reducer", t);
        }    
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (useMinId) {
            context.write(Question5AnswerJob.MIN_ID_KEY, new LongWritable(minId));
        }
        else if (useMaxId) {
            context.write(Question5AnswerJob.MAX_ID_KEY, new LongWritable(maxId));
        }
    }

}
