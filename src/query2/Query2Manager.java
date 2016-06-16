package query2;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import util.Constants;
import util.Logger;
import util.Statistics;
import util.Time;
import util.Util;
import management.ManagementMain;
import management.SerializerQ2;
import common.events.AbstractEvent;
import common.events.Comment;
import common.events.CommentExpires;
import common.events.EventType;
import common.events.Friendship;
import common.events.Like;
import common.events.Marker;
import common.events.StreamClosed;

/**
 * TODO: store both timestamps in events so that no back-convertion is needed!
 * Receives event streams friendship (last TS: 1350740269000), likes, comments and starts workers to find comments
 * with largest "like-cliques".
 * @author mayercn
 *
 */
public class Query2Manager extends Thread {

	private Time time;
	private Graph graph;
	private SerializerQ2 serializer;
	
	private List<CommentProcessor> lastTopK = new ArrayList<CommentProcessor>();
	private List<CommentProcessor> topK = new ArrayList<CommentProcessor>();
	
	private BufferedWriter out = null;
	private Logger eventLatencyLogger = new Logger("eventLatenciesQ2.dat");
	
	/**
	 * Variables to maintain the average latency per event
	 */
	private long latencyPerEventSummed = 0;
	private long numberOfEvents = 0;
	
	/**
	 * Maps comment ids to active comments, i.e., comments that were liked.
	 */
	private HashMap<Long, CommentProcessor> id2activeComments = new HashMap<Long, CommentProcessor>(1000);
	
	/**
	 * Maps comment ids to passive comments, i.e., comments that have no like at all.
	 */
	private HashMap<Long, CommentProcessor> id2passiveComments = new HashMap<Long, CommentProcessor>(1000);
	

	public Query2Manager(SerializerQ2 serializer) {
		time = new Time();
		graph = new Graph();
		this.serializer = serializer;
	}
	
	@Override
	public void run() {
//		System.out.println("DEBUG: starting the query2 manager");
//		ArrayBlockingQueue<AbstractEvent> inQ = Serializer.query2Queue;
		LinkedList<AbstractEvent> expiresQ = new LinkedList<AbstractEvent>();
		
		AbstractEvent nextEvent = null;
		AbstractEvent inQEvent = null;
		AbstractEvent expiresEvent = null;
		
		long expiresAfter = Constants.Q2_D*1000;
		
		/*
		 * initialize out stream 
		 */
		try {
			out = new BufferedWriter(new OutputStreamWriter(
			          new FileOutputStream(Constants.Q2_OUTFILE), "utf-8"));
		} catch (UnsupportedEncodingException | FileNotFoundException e1) {
			e1.printStackTrace();
		}

		runloop:
		while (true) {
			/*
			 * Determine next event, i.e., the event with smallest TS of both queues
			 */
			if (inQEvent==null) {
//					inQEvent = inQ.take();			// block and wait for next event, remove it
				inQEvent = serializer.getNextEvent();

				/*
				 * Directly add expires event before a comment is processed.
				 */
				if (inQEvent.getType()==EventType.COMMENT) {
					// add new expires comment event to in queue
					Comment comment = (Comment) inQEvent;
					long expiresTs = comment.getTs()+expiresAfter;
					CommentExpires expires = new CommentExpires(expiresTs, comment.getComment_id(),
							comment.getCreationTime());
					if (expiresEvent==null) {
						expiresEvent = expires;
					} else {
						expiresQ.add(expires);
					}
				}
			}
			if (expiresEvent==null) {
				expiresEvent = expiresQ.poll();	// poll for expiring event and remove it
			}
			if (expiresEvent!=null && expiresEvent.getTs()<inQEvent.getTs() 
					&& inQEvent.getType()!=EventType.STREAM_CLOSED) {
				nextEvent = expiresEvent;
				/*
				 *  set creation time of the comment expires event to the creation time of
				 *  the event that caused the comment to expire.
				 */
				nextEvent.setCreationTime(inQEvent.getCreationTime());
				expiresEvent = null;
			} else {
				nextEvent = inQEvent;
				inQEvent = null;
			}
			
//			System.out.println("Active comments: " + id2activeComments.size());
//			System.out.println("Passive comments: " + id2passiveComments.size());
			switch (nextEvent.getType()) {
			case FRIENDSHIP:
				Friendship friendship = (Friendship) nextEvent;
				processFriendship(friendship);
				break;
			case COMMENT:
				Comment comment = (Comment) nextEvent;
				processComment(comment);
				break;
			case COMMENT_EXPIRES:
				CommentExpires commentExpires = (CommentExpires) nextEvent;
				processCommentExpires(commentExpires);
				break;
			case LIKE:
				Like like = (Like) nextEvent;
				processLike(like);
				break;
			case STREAM_CLOSED:
				onLastEvent();	
				break runloop; // leave the run loop, shut down Query2Manager
			default:
				break;
			}
		}
		
//		System.out.println("Query2Manager thread shut down.");
	}
	

	/**
	 * Forward event to worker that is responsible for this comment.
	 * @param commentExpires
	 */
	private void processCommentExpires(CommentExpires commentExpires) {

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
		if (topK.contains(commentProcessor)) {
//			System.out.println("Comment " + commentProcessor + " removed; topK: " + localTopK);
			topK.remove(commentProcessor);
			// add comment with maximal range not being in topK list
			CommentProcessor newTopComment = null;
			if (id2activeComments.size()>0) {
				newTopComment = Util.getMax(id2activeComments.values(), topK);
			}
			if (newTopComment==null) {
				newTopComment = Util.getMax(id2passiveComments.values(), topK);
			}
			if (newTopComment!=null) {
				Util.sortedInsert(topK, newTopComment);
			}

			/*
			 * emit an update to the merger that the comment is deleted
			 */
			outputEvent(commentExpires);
		}
		
	}
	
	/**
	 * Update all comments with the new friendship request.
	 * @param friendship
	 */
	private void processFriendship(Friendship friendship) {

		
		graph.addVertex(friendship.getUser_id_1());
		graph.addVertex(friendship.getUser_id_2());
		graph.addEdge(friendship.getUser_id_1(), friendship.getUser_id_2());
		
		/*
		 * Determine the comments whose range have changed due to the changed graph.
		 * Change the local top k list based on the changed comments
		 * 
		 */
		int minRange = -1;
		boolean localTopKChanged = false;
		if (topK.size()>0) {
			minRange = topK.get(topK.size()-1).getLargestRange();
		}
		for (CommentProcessor commentproc : id2activeComments.values()){
			if (commentproc.onFriendship(friendship) && commentproc.getLargestRange()>=minRange) {
				// comment has changed and has high range
				if (topK.contains(commentproc)) {
					topK.remove(commentproc);	// if it was already present, the comment should be removed and re-inserted
				}
				Util.sortedInsert(topK, commentproc);
				minRange = topK.get(topK.size()-1).getLargestRange();
				localTopKChanged = true;
			}
		}
		
		/*
		 * output topK change
		 */
		if (localTopKChanged) {
			topK =  new ArrayList<CommentProcessor>(topK.subList(0, Math.min(topK.size(),Constants.Q2_K)));
			outputEvent(friendship);
		}
	}
	
	
	/**
	 * Add new comment to the data structures. As a new comment
	 * always has zero likes, it has no community in the first place.
	 * Therefore, we do not have to change topK here.
	 * @param comment
	 */
	private void processComment(Comment comment) {
		/*
		 * add comment to id2passivecomments (there could be no like at all)
		 */
		CommentProcessor p = new CommentProcessor(graph, comment);
		this.id2passiveComments.put(comment.getComment_id(), p);
		
		// a new comment can not be in topK list as its range is 0 (see test cases from DEBS website).
//		/*
//		 * update local topK list: new comment could be topK immediately
//		 */
//		if (topK.size()<Constants.Q2_K || topK.get(topK.size()-1).getLargestRange()==0) {
//			Util.sortedInsert(topK, p);
//			topK =  new ArrayList<CommentProcessor>(topK.subList(0, Math.min(topK.size(),Constants.Q2_K)));
//			/*
//			 * output topK change
//			 */
//			outputEvent(comment);
//		}
	}

	/**
	 * Update the worker thread that handles the comment to be liked.
	 * @param like
	 */
	private void processLike(Like like) {

		graph.addVertex(like.getUser_id());
		
		/*
		 * update the commentProcessor that is liked
		 */
		if (!id2activeComments.containsKey(like.getComment_id())) {
			// comment is getting active because it is liked
			CommentProcessor active = id2passiveComments.remove(like.getComment_id());
			// comment could be already expired, in this case active is null
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
		
		if (topK.size()==0 || likedComment.getLargestRange()>=topK.get(topK.size()-1).getLargestRange()) {
			if (topK.contains(likedComment)) {
				/*
				 * if it was already present, the comment should be
				 * reinserted as its position could have changed
				 */
				topK.remove(likedComment);
			}
			// add comment to topK list
			Util.sortedInsert(topK, likedComment);
			topK =  new ArrayList<CommentProcessor>(topK.subList(0, Math.min(topK.size(),Constants.Q2_K)));
			
			/*
			 * emit to merger
			 */
			outputEvent(like);
		}
	}
	

	public Graph getGraph() {
		return graph;
	}

	public void setGraph(Graph graph) {
		this.graph = graph;
	}
	
	/**
	 * TODO: If the last events comes in, we have to log the average processing time and total execution time
	 */
	private void onLastEvent() {
		long totalTime = (System.currentTimeMillis()-ManagementMain.getStartProcessingTime());
		double averageLatencyQ2 = (double)latencyPerEventSummed/(double)numberOfEvents;
		Statistics.setDurationQ2(totalTime);
		Statistics.setAverageLatencyQ2(averageLatencyQ2);
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		System.out.println("Total processing time: " + totalTime + " ms");
//		System.out.println("Average latency per out-event: " + ((double)latencyPerEventSummed/(double)numberOfEvents) + " ms");
	}
	
	/**
	 * Outputs a new event to the stream, if the topK list has changed
	 * @param event
	 * @param topK
	 */
	private void outputEvent(AbstractEvent event) {
		if (!Util.listEquals(topK, lastTopK)) { //TODO: do not compare strings for determining similarity of lists
			// list of local topK comments has changed
			lastTopK = Util.copy(topK);
			StringBuilder s = new StringBuilder(time.timeStamp(event.getTs()));
//			StringBuilder s;
//			if (event.getType()==EventType.COMMENT_EXPIRES) {
//				s = new StringBuilder(time.timeStamp1(event.getTs()));
//			} else {
//				s = new StringBuilder(event.getTs_string());
//			}
//			String s = time.timeStamp(event.getTs());
			for (int i=0; i<Constants.Q2_K; i++) {
				if (topK.size()>i) {
					s.append(",");
					s.append(topK.get(i).getComment().getComment());
//					s += "-&- " + topK.get(i).getComment().getComment() + "(" + topK.get(i).getLargestRange() + ")";
//					if (event.getTs()>topK.get(i).getComment().getTs()+Constants.Q2_D*1000) {
//						// comment should be expired already
//						System.err.println("Error: Comment should already be expired!!");
//						System.exit(-1);
//					}
				} else {
					s.append(",-");
//					s += "-&- -" + "(0)";
				}
			}
			try {
				// latency measurement
				numberOfEvents++;
				s.append(System.lineSeparator());
				long currentEventLatency = System.nanoTime()-event.getCreationTime();
				latencyPerEventSummed += currentEventLatency;
				double averageLatencyQ2 = (double)latencyPerEventSummed/(double)numberOfEvents;
				eventLatencyLogger.log(System.nanoTime() + "\t" + currentEventLatency + "\n");
				out.write(s.toString());
//				out.flush();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
}
