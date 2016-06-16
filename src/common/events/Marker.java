package common.events;

public class Marker extends AbstractEvent {

	public Marker(){
		this.setType(EventType.MARKER);
	}
	
	public Marker(long timestamp, long creationTime) {
		this.setType(EventType.MARKER);
		this.setTs(timestamp);
		this.setCreationTime(creationTime);
	}
}
