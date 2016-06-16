package query2_old;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import management.ManagementMain;
import util.Constants;
import common.events.AbstractEvent;
import common.events.Comment;
import common.events.CommentExpires;
import common.events.EventType;
import common.events.Friendship;
import common.events.Like;
import common.events.Marker;

public class Query2WorkerThread extends Thread {
	
	private static int workerIdCounter = 0; // to assign incremental ids to the workers upon construction
	private int workerId;
	private LinkedBlockingQueue<AbstractEvent> inQueue;
	private LinkedBlockingQueue<TopKUpdate> outQueue;
	
	/**
	 * Maps comment ids to active comments, i.e., comments that were liked.
	 */
	private HashMap<Long, CommentProcessor> id2activeComments = new HashMap<Long, CommentProcessor>();
	
	/**
	 * Maps comment ids to passive comments, i.e., comments that have no like at all.
	 */
	private HashMap<Long, CommentProcessor> id2passiveComments = new HashMap<Long, CommentProcessor>();
	private Query2Manager manager;
	
	// each worker maintains a local list of topK comments
	private List<CommentProcessor> localTopK = new ArrayList<CommentProcessor>();
	private List<CommentProcessor> oldTopK = new ArrayList<CommentProcessor>();
	private Graph graph;
	
	public Query2WorkerThread(Query2Manager manager) {
		this.manager = manager;
		this.inQueue = new LinkedBlockingQueue<AbstractEvent>(100);
		this.outQueue = new LinkedBlockingQueue<TopKUpdate>(100);
		setWorkerId(workerIdCounter++);
		if (Constants.Q2_REPLICATE_GRAPH) {
			graph = new Graph();
		} else {
			graph = manager.getGraph();
		}
	}
	
	/**
	 * Creates a new event topKUpdate and puts it into the outQueue.
	 * @param event
	 * @param topK
	 */
	private void sendTopKUpdate(AbstractEvent event, List<CommentProcessor> topK) {
		if (!Util.listEquals(topK, oldTopK)) { //TODO: do not compare strings for determining similarity of lists
			// list of local topK comments has changed
			TopKUpdate u = new TopKUpdate(event.getTs(), Util.copyCommentProcessorsIgnoreLikes(topK), 
					event.getType(), event.getCreationTime());
			oldTopK = Util.copyCommentProcessorsIgnoreLikes(topK);
			try {
				outQueue.put(u);
//				System.out.println("OutQ size: " + outQueue.size());
//				System.out.println("InQ size: " + inQueue.size());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Add the new comment to the comments data structure.
	 * As the new comment could be part of topK, update local
	 * topK list. Finally, the comment is sent to merger via outQ.
	 * @param comment
	 */
	private void handleComment(Comment comment){
		/*
		 * add comment to id2passivecomments (there could be no like at all)
		 */
		CommentProcessor p = new CommentProcessor(graph, comment);
		this.id2passiveComments.put(comment.getComment_id(), p);
		
		/*
		 * update local topK list: new comment could be topK immediately
		 */
		if (localTopK.size()<Constants.Q2_K || localTopK.get(localTopK.size()-1).getLargestRange()==0) {
			Util.sortedInsert(localTopK, p);
			localTopK =  new ArrayList<CommentProcessor>(localTopK.subList(0, Math.min(localTopK.size(),Constants.Q2_K)));
			/*
			 * send the comment to merger
			 */
			sendTopKUpdate(comment, localTopK);
		}
		
		
	}
	
	/**
	 * When a comment expires, it can be removed from the data structure.
	 * Also the local topK list can change due to the removal.
	 * @param commentExpires
	 */
	private void handleCommentExpires(CommentExpires commentExpires){
		
		/*
		 * remove the comment
		 */
		CommentProcessor commentProcessor;
		if (this.id2activeComments.containsKey(commentExpires.getComment_id())) {
			commentProcessor = this.id2activeComments.remove(commentExpires.getComment_id());
		} else {
			commentProcessor = this.id2passiveComments.remove(commentExpires.getComment_id());
		}
		
		/*
		 * update local topK list, if comment has been in it
		 */
		if (localTopK.contains(commentProcessor)) {
//			System.out.println("Comment " + commentProcessor + " removed; topK: " + localTopK);
			localTopK.remove(commentProcessor);
			// add comment with maximal range not being in topK list
			if (id2activeComments.size()>0) {
				CommentProcessor newTopComment = Util.getMax(id2activeComments.values(), localTopK);
				if (newTopComment!=null) {
					Util.sortedInsert(localTopK, newTopComment);
				}
			} else {
				CommentProcessor newTopComment = Util.getMax(id2passiveComments.values(), localTopK);
				if (newTopComment!=null) {
					Util.sortedInsert(localTopK, newTopComment);
				}
			}
//			System.out.println("Comment " + commentProcessor + " removed; topK: " + localTopK);
//			System.exit(-1);
			/*
			 * emit an update to the merger that the comment is deleted
			 */
			sendTopKUpdate(commentExpires, localTopK);
		}
		
	}

	/**
	 * The graph has already changed in the Query2Manager. Due to this change,
	 * potentially all active comments have to adapt their largest range. The local topK
	 * list can change due to this event.
	 * @param friendship
	 */
	private void handleFriendship(Friendship friendship){
		
		/*
		 * Update graph, if it is replicated on this worker
		 */
		if (Constants.Q2_REPLICATE_GRAPH) {
			graph.addVertex(friendship.getUser_id_1());
			graph.addVertex(friendship.getUser_id_2());
			graph.addEdge(friendship.getUser_id_1(), friendship.getUser_id_2());
		}
		
		/*
		 * Determine the comments whose range have changed due to the changed graph
		 */
		List<CommentProcessor> candidatesForTopK = new ArrayList<CommentProcessor>();
//		System.out.println(id2activeComments);
		for (CommentProcessor commentproc : id2activeComments.values()){
			if (commentproc.onFriendship(friendship)) {
				candidatesForTopK.add(commentproc);
			}
		}
//		System.out.println(candidatesForTopK.size());
		
		/*
		 * Change the local top k list based on the changed comments
		 */
		int minRange = -1;
		boolean localTopKChanged = false;
		if (localTopK.size()>0) {
			minRange = localTopK.get(localTopK.size()-1).getLargestRange();
		}
		for (CommentProcessor p : candidatesForTopK) {
			if (p.getLargestRange()>=minRange) {
				if (localTopK.contains(p)) {
					localTopK.remove(p);	// if it was already present, the comment should be removed and re-inserted
				}
				Util.sortedInsert(localTopK, p);
				localTopK =  new ArrayList<CommentProcessor>(localTopK.subList(0, Math.min(localTopK.size(),Constants.Q2_K)));
				localTopKChanged = true;
			}
		}
		
		/*
		 * emit to merger
		 */
		if (localTopKChanged) {
			sendTopKUpdate(friendship, localTopK);
		}
	}
	
	/**
	 * A like can only change one comments' range. But the local topK list
	 * can change due to this like (but only adding the changed comment,
	 * if it is not already added).
	 * @param like
	 */
	private void handleLike(Like like){
		
		if (Constants.Q2_REPLICATE_GRAPH) {
			graph.addVertex(like.getUser_id());
		}
		
		/*
		 * update the commentProcessor that is liked
		 */
		if (!id2activeComments.containsKey(like.getComment_id())) {
			// comment is getting active because it is liked
			CommentProcessor active = id2passiveComments.remove(like.getComment_id());
			// comment could be already expired
			if (active!=null) {
				id2activeComments.put(like.getComment_id(), active);
			}
		}
		CommentProcessor likedComment = this.id2activeComments.get(like.getComment_id());
		if (likedComment == null) {
			// comment has already expired -> ignore
			return;
		}
		likedComment.onLike(like);
		
		if (likedComment.getLargestRange()>=localTopK.get(localTopK.size()-1).getLargestRange() && 
				!localTopK.contains(likedComment)) {
			// add comment to topK list
			Util.sortedInsert(localTopK, likedComment);
			localTopK =  new ArrayList<CommentProcessor>(localTopK.subList(0, Math.min(localTopK.size(),Constants.Q2_K)));
			
			/*
			 * emit to merger
			 */
			sendTopKUpdate(like, localTopK);
		}
		
	}
	
	private void handleMarker(Marker marker){
		manager.markerCallback();
	}
	
	
	@Override
	public void run() {
		int marker = 0;
		int comment = 0;
		int comment_expires = 0;
		int friendship = 0;
		int like = 0;
		int total = 0;
		long latestTs = 0;
		runloop:
		while (true){
			try {
				AbstractEvent nextEvent = inQueue.take();
				
				total++;
//				System.out.println("inQ size: " + inQueue.size());
//				System.out.println("outQ size: " + outQueue.size());
//				System.out.println("next event: " + nextEvent.getType());
//				System.out.println("topK: " + localTopK);
//				System.out.println("active comments: " + id2comments.size());
//				System.out.println("comments: " + comment);
//				System.out.println("comment_expires: " + comment_expires);
//				System.out.println("friendship: " + friendship);
//				System.out.println("like: " + like);
//				System.out.println("marker: " + marker);
//				System.out.println("total: " + total);
//				System.out.println("average processing time per event: " 
//						+ (double)(System.currentTimeMillis()-ManagementMain.getStartProcessingTime())/(double)total + " ms");
//				System.out.println("");
				if (nextEvent.getTs()>=latestTs) {
					latestTs = nextEvent.getTs();
				} else if (nextEvent.getType()!=EventType.MARKER){
					System.err.println("Worker " + workerId + " received event with smaller TS!");
					System.err.println("Event ts: " + nextEvent.getTs());
					System.err.println("latest ts: " + latestTs);
					System.err.println("nextEvent.type: " + nextEvent.getType());
					System.exit(-1);
				}
				
				switch (nextEvent.getType()){
				case COMMENT:
					comment++;
					this.handleComment((Comment) nextEvent);
					break;
				case COMMENT_EXPIRES:
					comment_expires++;
					this.handleCommentExpires((CommentExpires) nextEvent);
					break;
				case FRIENDSHIP:
					friendship++;
					this.handleFriendship((Friendship) nextEvent);
					break;
				case LIKE:
					like++;
					this.handleLike((Like) nextEvent);
					break;				
				case MARKER:
					marker++;
					this.handleMarker((Marker) nextEvent);
					break;
				case STREAM_CLOSED:
					outQueue.put(new TopKUpdate(Long.MAX_VALUE, null, EventType.STREAM_CLOSED, System.currentTimeMillis()));
					break runloop; // shut down the worker thread
				default:
					break;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
//		System.out.println("Query2Worker thread " + this.workerId + " shut down.");
	}

	public LinkedBlockingQueue<AbstractEvent> getInQueue() {
		return inQueue;
	}
	
	public LinkedBlockingQueue<TopKUpdate> getOutQueue() {
		return outQueue;
	}

	public int getWorkerId() {
		return workerId;
	}

	private void setWorkerId(int workerId) {
		this.workerId = workerId;
	}
}
