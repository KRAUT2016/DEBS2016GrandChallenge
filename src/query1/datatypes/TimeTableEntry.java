package query1.datatypes;

import java.util.concurrent.atomic.AtomicLong;

import common.events.Post;
import util.Constants;

/**
 * Entries in the time table of a post
 * 
 * they build a single-linked list that is sorted by timestamp
 * 
 * @author mayerrn
 *
 */
public class TimeTableEntry {
	
	private long timestamp; // timestamp of when the entry was created -- each 24 h from there on, the ttl is decremented
	private Post post; // post that this timetable entry is associated with
	
	private long commenter_user_id = Long.MIN_VALUE; // the user id of the commenter, if this time table entry is associated to a comment
	
//	private long post_id; // post_id this timetable entry is associated with
	private byte ttl; // time to live (in days)
	public byte getTtl() {
		return ttl;
	}

	public void setTtl(byte ttl) {
		this.ttl = ttl;
	}

	private TimeTableEntry successor;
	
//	public TimeTableEntry(long timestamp, long post_id){
//		this.id = idCounter.getAndIncrement();
//		this.timestamp = timestamp;
//		this.ttl = (byte) 10;
//		this.setPost_id(post_id);
//	}
	
	public TimeTableEntry(long timestamp, Post post){
		this.timestamp = timestamp;
		this.ttl = (byte) 10;
		this.post = post;
	}

	/**
	 * replace the current successor with a new successor;
	 * the old successor becomes the successor of the new successor next 
	 * @param newSucessor the new successor of this entry
	 */
	public void addNewSuccessor(TimeTableEntry newSuccessor){
		newSuccessor.setSuccessor(this.successor);
		this.setSuccessor(newSuccessor);
	}
	
			
	/**
	 * the time of day (clock-time) the entry was produced
	 * 
	 * when this time is passed, the ttl is decremented 
	 * @return timestamp % 24 hours;
	 */
	public long getDayTime() {
		return this.timestamp % Constants.MS_24H;
	}
	
	public long getTimestamp() {
		return this.timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public TimeTableEntry getSuccessor() {
		return successor;
	}

	public void setSuccessor(TimeTableEntry successor) {
		this.successor = successor;
	}

	/**
	 * decrement TTL by 1;
	 * if new TTL is 0, i.e., the table entry becomes inactive, it returns true
	 * @return true if TTL reached 0; i.e., true if inactive.
	 */
	public boolean decrementTTL(){
		this.ttl--;
//		System.out.println("entry of post " + this.post.getPost_id() + " TTL " + ttl);
		if (this.ttl <= 0){
			return true;
		}
		else {
			return false;
		}
	}
	

	public Post getPost() {
		return post;
	}

	public void setPost(Post post) {
		this.post = post;
	}

	public long getCommenter_user_id() {
		return commenter_user_id;
	}

	public void setCommenter_user_id(long commenter_user_id) {
		this.commenter_user_id = commenter_user_id;
	}

	
	
}
