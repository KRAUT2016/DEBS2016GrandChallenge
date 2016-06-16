package query1.datatypes;

import java.util.HashMap;

public class UpdateNotification {
	private long creationTime; // creation time of the event that triggered the update
	private long timestamp; // timestamp for the output in merger / timestamp of the processing step
	private HashMap<Long, PostScore> updates; // <post_id, updated score>
	

	public UpdateNotification(long timestamp, HashMap<Long, PostScore> updates, long creationTime){
		this.timestamp = timestamp;
		this.updates = updates;
		this.setCreationTime(creationTime);
	}
	
	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public HashMap<Long, PostScore> getUpdates() {
		return updates;
	}

	public void setUpdates(HashMap<Long, PostScore> updates) {
		this.updates = updates;
	}

	public long getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}

	
}
