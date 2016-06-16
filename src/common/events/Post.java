package common.events;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import query1.Query1SingleThreaded;

public class Post extends AbstractEvent implements Comparable<Post>{
		
	private boolean updateThisRound = false;
	
	private boolean isInUnsortedSet = false;
	
	private ArrayList<Long> comments;
	private LinkedList<Long> commentTimestamps;
	private HashSet<Long> commenters;
	private int postScore = 0;
	private int oldPostScore = 0; // the score before an update in a progress step
	
	private long post_id;
	private long user_id;
	private String user;
	
	private final int hashCode;
		
	public Post(long ts, long post_id, long user_id, String user, long creationTime){
		this.comments = new ArrayList<Long>();
		this.commentTimestamps = new LinkedList<Long>();
		this.commenters = new HashSet<Long>();
		
		this.setType(EventType.POST);
		this.setTs(ts);
		this.post_id = post_id;
		this.user_id = user_id;
		this.user = user;
		this.hashCode = (int) post_id;
		
		this.commenters.add(user_id);
		this.setCreationTime(creationTime);
	}



	public long getPost_id() {
		return post_id;
	}

	public void setPost_id(long post_id) {
		this.post_id = post_id;
	}

	public long getUser_id() {
		return user_id;
	}

	public void setUser_id(long user_id) {
		this.user_id = user_id;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public ArrayList<Long> getComments() {
		return comments;
	}


	public void setComments(ArrayList<Long> comments) {
		this.comments = comments;
	}


	public LinkedList<Long> getCommentTimestamps() {
		return commentTimestamps;
	}


	public void setCommentTimestamps(LinkedList<Long> commentTimestamps) {
		this.commentTimestamps = commentTimestamps;
	}


	public HashSet<Long> getCommenters() {
		return commenters;
	}


	public void setCommenters(HashSet<Long> commenters) {
		this.commenters = commenters;
	}

	public boolean incrementPostScore(int increment){
		postScore += increment;

//		System.out.println("post " + this.post_id + " increment " + increment + " new score " + postScore);
		if (postScore == 0){
			return true;
		}
		else {
			return false;
		}
	}

	public int getPostScore() {
		return postScore;
	}

	public void setPostScore(int postScore) {
		this.postScore = postScore;
	}


	public int getOldPostScore() {
		return oldPostScore;
	}


	public void setOldPostScore(int oldPostScore) {
		this.oldPostScore = oldPostScore;
	}


	@Override
	public int compareTo(Post otherPost) {
		if (this.equals(otherPost)){
			return 0;
		}
		if (this.oldPostScore == otherPost.oldPostScore){
			
			if (this.getTs() > otherPost.getTs()){
				return 1;
			}
			else if (this.getTs() < otherPost.getTs()) {
				return -1;
			}
			else {
				if (this.post_id >= otherPost.getPost_id()){
					return 1;
				}
				else {
					return -1;
				}
			}
		}
		else {
			return (this.oldPostScore - otherPost.oldPostScore);
		}
	}
	
	@Override
	public int hashCode() {
		return hashCode;
	}


	public boolean isUpdateThisRound() {
		return updateThisRound;
	}


	public void setUpdateThisRound(boolean updateThisRound) {
		this.updateThisRound = updateThisRound;
	}


	public boolean isInUnsortedSet() {
		return isInUnsortedSet;
	}

	public void setInUnsortedSet(boolean isInUnsortedSet) {
		this.isInUnsortedSet = isInUnsortedSet;
	}
	
	public static void main(String[] args) {
		long time = System.currentTimeMillis();
		HashMap<Integer,Post> p = new HashMap<Integer,Post>();
		for (int j=0; j<1000000; j++) {
			Post post = new Post(j, j, 21321321321L, "user123", 324234324L);
			p.put(j,post);
		}
		System.out.println((System.currentTimeMillis()-time) + " milliseconds");
	}
	
}
