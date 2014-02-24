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

import java.io.IOException;

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
public class Question9Mapper1 extends BaseMapper<LongWritable, Text, LongWritable, LongWritable> {

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
        	Status status = DataObjectFactory.createStatus(value.toString());
        	User user = status.getUser();
        	if (user != null) {
        		Long userId = user.getId();
        		if (userId != null && userId != 0) {
        			UserMentionEntity[] userMentions = status.getUserMentionEntities();
        			if (userMentions != null) {
        				// Emit first levels as bidirectional
        				for (UserMentionEntity um : userMentions) {
        					context.write(new LongWritable(userId), new LongWritable(um.getId()));
        					context.write(new LongWritable(um.getId()), new LongWritable(userId));
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
