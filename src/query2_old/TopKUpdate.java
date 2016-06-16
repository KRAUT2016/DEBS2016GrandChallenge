package query2_old;

import java.util.List;

import common.events.EventType;

public class TopKUpdate {

	private EventType type;
	private long ts; 						// timestamp
	private List<CommentProcessor> topK;	// list of comments sorted by range/lexicographical order
	private long creationTime;				// system time of event that initiated this topK change
	
	public TopKUpdate(long ts, List<CommentProcessor> topK, EventType type, long creationTime) {
		this.ts = ts;
		this.topK = topK;
		this.type = type;
		this.setCreationTime(creationTime);
	}
	
	public List<CommentProcessor> getTopK() {
		return topK;
	}
	public void setTopK(List<CommentProcessor> topK) {
		this.topK = topK;
	}
	public long getTs() {
		return ts;
	}
	public void setTs(long ts) {
		this.ts = ts;
	}

	public EventType getType() {
		return type;
	}

	public void setType(EventType type) {
		this.type = type;
	}

	public long getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}

	
	
	
}
