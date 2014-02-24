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

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import tr.com.t2.hackathon.answers.Answers.BaseMapper;
import twitter4j.Status;
import twitter4j.User;
import twitter4j.json.DataObjectFactory;

/**
 * @author Serkan OZAL
 */
public class Question4Mapper extends BaseMapper<LongWritable, Text, NullWritable, IntWritable> {

    private final static NullWritable NO_KEY = NullWritable.get();
    
    private String lang;
    private boolean langParamExist;
    
    protected void init(Context context) {
        try {
            // Get language parameter from configuration
            lang = context.getConfiguration().get(Question4AnswerJob.LANGUAGE_PARAMETER_NAME);
            langParamExist = !StringUtils.isEmpty(lang);
            logger.info("Mapper has been initialized for language parameter " + lang + " ...");
        }
        catch (Throwable t) {
            logger.error("Error occured while initializing Mapper", t);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            Status status = DataObjectFactory.createStatus(value.toString());
            // If language parameter specified, process only tweets from user with specified language
            if (langParamExist) {
                User user = status.getUser();
                if (user != null) {
                    if (lang.equalsIgnoreCase(user.getLang())) {
                        // Since we just calculate number of values, only one reducer can be used
                        // Emit it with a constant key
                        context.write(NO_KEY, ONE);
                    }   
                }   
            }
            // If language parameter not specified, process all tweets
            else {
                // Since we just calculate number of values, only one reducer can be used
                // Emit it with a constant key
                context.write(NO_KEY, ONE);
            }
        }
        catch (Throwable t) {
            logger.error("Error occured while executing map function of Mapper", t);
        }
    }

}
