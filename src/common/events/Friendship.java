package common.events;

public class Friendship extends AbstractEvent {
		
	private long user_id_1;
	private long user_id_2;
	
	public Friendship(long ts, long user_id_1, long user_id_2, long creationTime){
		this.setType(EventType.FRIENDSHIP);
		this.setTs(ts);
		this.user_id_1 = user_id_1;
		this.user_id_2 = user_id_2;
		this.setCreationTime(creationTime);
	}
	
	public long getUser_id_1() {
		return user_id_1;
	}
	public void setUser_id_1(long user_id_1) {
		this.user_id_1 = user_id_1;
	}
	public long getUser_id_2() {
		return user_id_2;
	}
	public void setUser_id_2(long user_id_2) {
		this.user_id_2 = user_id_2;
	}
}
