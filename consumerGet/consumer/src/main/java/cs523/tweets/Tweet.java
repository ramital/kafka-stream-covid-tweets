package cs523.tweets;

import java.util.ArrayList;
import java.util.List;

public class Tweet {

	
	private String id;
	private String text;
	private boolean isRetweet;
	private String inReplyStsId;
	private List<String> hashTagList = new ArrayList<String>();
	private String username;
	private String timeStamp;
	private String lang;

	public String getId() 
	{
		return id;
	}

	public void setId(String id) 
	{
		this.id = id;
	}

	public String getText() 
	{
		return text;
	}

	public void setText(String text) 
	{
		this.text = text;
	}

	
	public void setRetweet(boolean isRetwt) 
	{
		this.isRetweet = isRetwt;
	}
	
	public boolean isRetweet() 
	{
		return isRetweet;
	}


	public String getInReplyToStatusId() 
	{
		return inReplyStsId;
	}

	public void setInReplyToStatusId(String inReplyStsId) 
	{
		this.inReplyStsId = inReplyStsId;
	}

	public List<String> getHashTags() 
	{
		return hashTagList;
	}

	public void setHashTags(List<String> hashTagList) 
	{
		this.hashTagList = hashTagList;
	}
	
	public void setUsername(String username) 
	{
		this.username = username;
	}

	public String getTimeStamp() 
	{
		return timeStamp;
	}

	public String getUsername() 
	{
		return username;
	}

	public void setTimeStamp(String timeStamp) 
	{
		this.timeStamp = timeStamp;
	}

	public String getLang() 
	{
		return lang;
	}

	public void setLang(String lang) 
	{
		this.lang = lang;
	}

	@Override
	public String toString() {
		return "Tweet [id=" + id + ", text=" + text + ", isRetweet=" + isRetweet + ", inReplyToStatusId=" + inReplyStsId
				+ ", hashTags=" + hashTagList + ", username=" + username 	+ ", timeStamp=" + timeStamp + ", lang=" + lang + "]";
	}

	
}
