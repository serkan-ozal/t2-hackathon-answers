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

import java.io.Serializable;

/**
 * @author Serkan OZAL
 */
@SuppressWarnings("serial")
public class MentionedTweetData implements Serializable {

    private long senderId;
    private long senderTweetCount;
    private long senderFollowerCount;
    private long mentionedId;
    
    public MentionedTweetData() {
        
    }
    
    public MentionedTweetData(long senderId, long senderTweetCount, long senderFollowerCount, long mentionedId) {
        this.senderId = senderId;
        this.senderTweetCount = senderTweetCount;
        this.senderFollowerCount = senderFollowerCount;
        this.mentionedId = mentionedId;
    }
    
    public long getSenderId() {
        return senderId;
    }
    
    public void setSenderId(long senderId) {
        this.senderId = senderId;
    }
    
    public long getSenderTweetCount() {
        return senderTweetCount;
    }
    
    public void setSenderTweetCount(long senderTweetCount) {
        this.senderTweetCount = senderTweetCount;
    }
    
    public long getSenderFollowerCount() {
        return senderFollowerCount;
    }
    
    public void setSenderFollowerCount(long senderFollowerCount) {
        this.senderFollowerCount = senderFollowerCount;
    }
    
    public long getMentionedId() {
        return mentionedId;
    }
    
    public void setMentionedId(long mentionedId) {
        this.mentionedId = mentionedId;
    }
    
}
