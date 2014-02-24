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

import tr.com.t2.hackathon.answers.Answers.BaseMapper;
import twitter4j.Status;
import twitter4j.User;
import twitter4j.json.DataObjectFactory;

/**
 * @author Serkan OZAL
 */
public class Question5Mapper extends BaseMapper<LongWritable, Text, Text, LongWritable> {

    private Long minId;
    private Long maxId;
    private User minIdUser;
    private User maxIdUser;
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            Status status = DataObjectFactory.createStatus(value.toString());
            User user = status.getUser();
            if (user != null) {
                // If minimum id has been set or current id is smaller than minimum id, set current id as minimum id
                if (minId == null || user.getId() < minId) {
                    minId = user.getId();
                    minIdUser = user;
                }
                // If maximum id has been set or current id is bigger than maximum id, set current id as maximum id
                if (maxId == null || user.getId() > maxId) {
                    maxId = user.getId();
                    maxIdUser = user;
                }
            }
        }
        catch (Throwable t) {
            logger.error("Error occured while executing map function of Mapper", t);
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // If minimum id has been found, emit it to reducer assigned for calculating minimum id
        if (minIdUser != null) {
            context.write(Question5AnswerJob.MIN_ID_KEY, new LongWritable(minId));
        }
        // If maximum id has been found, emit it to reducer assigned for calculating maximum id
        if (maxIdUser != null) {
            context.write(Question5AnswerJob.MAX_ID_KEY, new LongWritable(maxId));
        }
    }

}
