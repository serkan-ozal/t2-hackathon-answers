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

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import tr.com.t2.hackathon.answers.Answers.BaseMapper;

/**
 * @author Serkan OZAL
 */
public class Question2Mapper extends BaseMapper<LongWritable, Question2Data, LongWritable, LongWritable> {

	@Override
    protected void map(LongWritable key, Question2Data value, Context context) throws IOException, InterruptedException {
        try {
        	// Emit group id as key and amount as value
        	context.write(new LongWritable(value.getGroupId()), new LongWritable(value.getAmount()));
        }
        catch (Throwable t) {
        	logger.error("Error occured while executing map function of Mapper", t);
        }
    }

}
