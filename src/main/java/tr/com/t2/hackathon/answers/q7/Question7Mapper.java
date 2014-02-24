/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import tr.com.t2.hackathon.answers.Answers.BaseMapper;
import twitter4j.Status;
import twitter4j.User;
import twitter4j.UserMentionEntity;
import twitter4j.json.DataObjectFactory;

/**
 * @author Serkan OZAL
 */
public class Question7Mapper extends BaseMapper<LongWritable, Text, LongWritable, IntWritable> {

	private final static IntWritable PLUS_ONE = new IntWritable(+1);
	private final static IntWritable MINUS_ONE = new IntWritable(-1);
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
        	Status status = DataObjectFactory.createStatus(value.toString());
        	User user = status.getUser();
        	if (user != null) {
        		Long userId = user.getId();
        		if (userId != null && userId != 0) {
        			// Decrease score of sender user 
        			context.write(new LongWritable(userId), MINUS_ONE);
        			UserMentionEntity[] userMentions = status.getUserMentionEntities();
        			if (userMentions != null) {
        				for (UserMentionEntity um : userMentions) {
        					// Increase score of mentioned user 
        					context.write(new LongWritable(um.getId()), PLUS_ONE);
        				}
        			}
        		}
        	}
        }
        catch (Throwable t) {
        	logger.error("Error occured while executing map function of Mapper", t);
        }
    }

}
