/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q1;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import tr.com.t2.hackathon.answers.Answers.BaseMapper;
import twitter4j.Place;
import twitter4j.Status;
import twitter4j.json.DataObjectFactory;

/**
 * @author Serkan OZAL
 */
public class Question1Mapper extends BaseMapper<LongWritable, Text, Text, IntWritable> {

	private Text word = new Text();
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
        	Status status = DataObjectFactory.createStatus(value.toString());
        	String countryCode = null;
        	Place place = status.getPlace();
        	// If place is not empty, get is country code
        	if (place != null) {
        		countryCode = place.getCountryCode();
        	}
        	// If country code is empty, assume its country code as unknown and use "??"
        	if (StringUtils.isEmpty(countryCode)) {
        		word.set("??");
        	}
        	// Else, country code country code of place
        	else {
        		word.set(countryCode);
        	}
        	// This country code occurred once for this tweet
        	context.write(word, ONE);
        }
        catch (Throwable t) {
        	logger.error("Error occured while executing map function of Mapper", t);
        }
    }

}
