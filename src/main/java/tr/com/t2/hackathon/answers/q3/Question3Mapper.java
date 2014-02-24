/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import tr.com.t2.hackathon.answers.Answers.BaseMapper;
import twitter4j.Status;
import twitter4j.User;
import twitter4j.json.DataObjectFactory;

/**
 * @author Serkan OZAL
 */
public class Question3Mapper extends BaseMapper<LongWritable, Text, LongWritable, IntWritable> {

    private LongWritable ID = new LongWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            Status status = DataObjectFactory.createStatus(value.toString());
            User user = status.getUser();
            if (user != null) {
                ID.set(user.getId());
                // This is occurred one time for this tweet
                context.write(ID, ONE);
            }
        }
        catch (Throwable t) {
            logger.error("Error occured while executing map function of Mapper", t);
        }
    }

}
