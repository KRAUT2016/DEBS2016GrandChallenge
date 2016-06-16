package common.events;

public class Comment extends AbstractEvent {
		
	private long comment_id;
	private long user_id;
	private String comment;
	private String user;
	private long comment_replied;
	private long post_commented;
	
	private Post post; // direct reference to the commented post object
	
	public Comment(long ts, long comment_id, long user_id, String comment,
			String user, long comment_replied, long post_commented, long creationTime){
		this.setType(EventType.COMMENT);
		this.setTs(ts);
		this.setComment_id(comment_id);
		this.setUser_id(user_id);
		this.setComment(comment);
		this.setUser(user);
		this.setComment_replied(comment_replied);
		this.setPost_commented(post_commented);
		this.setCreationTime(creationTime);
		
	}

	public long getComment_id() {
		return comment_id;
	}

	public void setComment_id(long comment_id) {
		this.comment_id = comment_id;
	}

	public long getUser_id() {
		return user_id;
	}

	public void setUser_id(long user_id) {
		this.user_id = user_id;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public long getComment_replied() {
		return comment_replied;
	}

	public void setComment_replied(long comment_replied) {
		this.comment_replied = comment_replied;
	}

	public long getPost_commented() {
		return post_commented;
	}

	public void setPost_commented(long post_commented) {
		this.post_commented = post_commented;
	}

	public Post getPost() {
		return post;
	}

	public void setPost(Post post) {
		this.post = post;
	}
}
