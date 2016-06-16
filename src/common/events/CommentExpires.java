package common.events;

public class CommentExpires extends AbstractEvent {
	
	private long comment_id;
	
	public CommentExpires(long ts, long comment_id, long creationTime){
		this.setType(EventType.COMMENT_EXPIRES);
		this.setTs(ts);
		this.setComment_id(comment_id);
		this.setCreationTime(creationTime);
	}

	public long getComment_id() {
		return comment_id;
	}

	public void setComment_id(long comment_id) {
		this.comment_id = comment_id;
	}
}
