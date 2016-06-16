package common.events;

public class Like extends AbstractEvent {
		
	private long user_id;
	private long comment_id;
	
	public Like(long ts, long user_id, long comment_id, long creationTime){
		this.setType(EventType.LIKE);
		this.setTs(ts);
		this.setUser_id(user_id);
		this.setComment_id(comment_id);
		this.setCreationTime(creationTime);
	}

	public Like() {
		// TODO Auto-generated constructor stub
	}

	public long getUser_id() {
		return user_id;
	}

	public void setUser_id(long user_id) {
		this.user_id = user_id;
	}

	public long getComment_id() {
		return comment_id;
	}

	public void setComment_id(long comment_id) {
		this.comment_id = comment_id;
	}
}
