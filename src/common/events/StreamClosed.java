package common.events;

public class StreamClosed extends AbstractEvent {

	public StreamClosed(){
		this.setTs(Long.MAX_VALUE);
		this.setType(EventType.STREAM_CLOSED);
	}
}
