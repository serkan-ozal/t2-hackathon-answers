/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q6;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import tr.com.t2.hackathon.answers.Answers.BaseReducer;

/**
 * @author Serkan OZAL
 */
public class Question6Reducer extends BaseReducer<LongWritable, MentionedTweetData, LongWritable, LongWritable> {

    protected void reduce(LongWritable key, Iterable<MentionedTweetData> values, Context context) throws IOException, InterruptedException {
        try {
            long score = 0;
            // Calculate score of current mentioned user
            for (MentionedTweetData value : values) {
                score += 
                        (
                            value.getSenderTweetCount() * value.getSenderFollowerCount()
                        );
            }
            // Emit score of current mentioned user
            context.write(key, new LongWritable(score));
        }
        catch (Throwable t) {
            logger.error("Error occured while executing reduce function of Reducer", t);
        }    
    }
    
}
