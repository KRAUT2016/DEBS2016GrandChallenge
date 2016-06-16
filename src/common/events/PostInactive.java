package common.events;

public class PostInactive extends AbstractEvent {
	private long post_id;
	
	public PostInactive(long post_id){
		this.post_id = post_id;
	}

	public long getPost_id() {
		return post_id;
	}

	public void setPost_id(long post_id) {
		this.post_id = post_id;
	}
}
