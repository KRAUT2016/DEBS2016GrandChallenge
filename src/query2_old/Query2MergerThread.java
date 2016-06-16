package query2_old;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import management.ManagementMain;
import util.Constants;
import util.Statistics;
import util.Time;
import common.events.AbstractEvent;
import common.events.EventType;

/**
 * Merges the workers outputStreams into the query2 output stream
 * consisting of all events that have changed the topK order.
 * @author mayercn
 *
 */
public class Query2MergerThread extends Thread {

	/**
	 * this array contains the first event from each incoming queue
	 * (friendships, posts, comments, likes)
	 * The serializer decides which event to take next, based on 
	 * comparison of the first events, and also puts the next 
	 * first event into the queue
	 */
	private static TopKUpdate[] firstEvents = new TopKUpdate[Constants.Q2_NUM_WORKERS];
	
	/**
	 * This array is used to remember the old topK lists of each worker. Based
	 *  on this information, the global topK can be computed.
	 */
	private static TopKUpdate[] lastEvents = new TopKUpdate[Constants.Q2_NUM_WORKERS];
	
	/**
	 * This array of all worker threads.
	 */
	private Query2WorkerThread[] workers = new Query2WorkerThread[Constants.Q2_NUM_WORKERS];
	
	private List<CommentProcessor> lastTopK = new ArrayList<CommentProcessor>();
	private List<CommentProcessor> topK = new ArrayList<CommentProcessor>();
	private Time time;
	
	private BufferedWriter out = null;
	
	/**
	 * Variables to maintain the average latency per event
	 */
	private long latencyPerEventSummed = 0;
	private long numberOfEvents = 0;
	
	public Query2MergerThread(List<Query2WorkerThread> workers) {
		time = new Time();
		int i=0;
		for (Query2WorkerThread worker : workers) {
			this.workers[i] = worker;
			i++;
		}
	}
	
	@Override
	public void run() {
		
		/*
		 * initialize out stream 
		 */
		
		try {
			out = new BufferedWriter(new OutputStreamWriter(
			          new FileOutputStream(Constants.Q2_OUTFILE), "utf-8"));
		} catch (UnsupportedEncodingException | FileNotFoundException e1) {
			e1.printStackTrace();
		}
		
		/*
		 * Load the first events from each queue
		 */
		try {
			for (int i=0; i<Constants.Q2_NUM_WORKERS; i++) {
				firstEvents[i] = workers[i].getOutQueue().take();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		runloop:
		while (true) {
			long lowestTimestamp = Long.MAX_VALUE;
			byte indexOfNextEvent = 0;
			
			// check which of the firstEvents has the lowest timestamp
			for (byte i = 0; i < Constants.Q2_NUM_WORKERS; i++){
				if (firstEvents[i].getTs() < lowestTimestamp){
					indexOfNextEvent = i;
					lowestTimestamp = firstEvents[i].getTs();
				}
			}
			
			TopKUpdate nextEvent = firstEvents[indexOfNextEvent];
//			System.out.println("Next event merger: " + nextEvent.getTs() + " " + nextEvent.getType() + " " + nextEvent.getTopK());
			if (nextEvent.getType()==EventType.STREAM_CLOSED){
				onLastEvent();
				try {
					this.out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				break runloop; // shut down the merger
			} else if (nextEvent.getType()==EventType.FRIENDSHIP) {
				
				// the firstEvents data structure is updated: read all friendship events and store them
				try {
					for (int i=0; i<Constants.Q2_NUM_WORKERS; i++) {
						if (firstEvents[i].getType()==EventType.FRIENDSHIP
								&& firstEvents[i].getTs()==nextEvent.getTs()) {
							lastEvents[i] = firstEvents[i];
							firstEvents[i] = workers[i].getOutQueue().take();
						}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				
				// the firstEvents data structure is updated, remember old topK lists
				try {
					lastEvents[indexOfNextEvent] = nextEvent;
					firstEvents[indexOfNextEvent] = workers[indexOfNextEvent].getOutQueue().take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} 
			}
			topK = determineTopK();
			
			/*
			 * Output the new topK comments, if they have changed.
			 */
			if (!Util.listEqualCommentIDs(lastTopK, topK)) {
//				System.out.println();
//				System.out.println("last top k: " + lastTopK);
//				System.out.println("top k: " + topK);
				// output has changed
				outputNextTuple(nextEvent, out);
				lastTopK = Util.copyCommentProcessorsIgnoreLikes(topK);	//TODO: copy only refs is sufficient here
			}
		}
		
//		System.out.println("Query2 Merger shut down.");
	}

	/**
	 * Calculates the top k comments based on the workers
	 * last local top k lists.
	 * @return
	 */
	private List<CommentProcessor> determineTopK() {
		List<CommentProcessor> top = new ArrayList<CommentProcessor>();
		for (int i=0; i<Constants.Q2_NUM_WORKERS; i++) {
			if (lastEvents[i]!=null) {
				for (CommentProcessor p : lastEvents[i].getTopK()) {
					Util.sortedInsert(top, p);
					// TODO: top = top.subList(0, Math.min(top.size(), Constants.Q2_K));
				}
			}
		}
		return new ArrayList<CommentProcessor>(top.subList(0, Math.min(top.size(),Constants.Q2_K)));
	}
	
	/**
	 * Prints the query2 output into the file.
	 * @param topK
	 * @param oldTopK
	 * @param ts
	 * @param out
	 */
	private void outputNextTuple(TopKUpdate e, BufferedWriter out) {
		

		
		// log change
		String s = time.timeStamp(e.getTs());
		for (int i=0; i<Constants.Q2_K; i++) {
			if (topK.size()>i) {
//				s += ", t=" + topK.get(i).getComment().getTs() + "--" + topK.get(i).getComment().getComment_id() + ": " + topK.get(i).getComment().getComment() + " (" + topK.get(i).getLargestRange() + ")";
				s += ", " + topK.get(i).getComment().getComment();

			} else {
				s += ", -";
			}
		}
		try {
//			System.out.println("Query2 output: " + s);
			// latency measurement
			numberOfEvents++;
			long currentEventLatency = System.nanoTime()-e.getCreationTime();
			latencyPerEventSummed += currentEventLatency;
			out.write(s + "\n");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * TODO: If the last events comes in, we have to log the average processing time and total execution time
	 */
	private void onLastEvent() {
		long totalTime = (System.currentTimeMillis()-ManagementMain.getStartProcessingTime());
		double averageLatencyQ2 = (double)latencyPerEventSummed/(double)numberOfEvents;
		Statistics.setDurationQ2(totalTime);
		Statistics.setAverageLatencyQ2(averageLatencyQ2);
//		System.out.println("Total processing time: " + totalTime + " ms");
//		System.out.println("Average latency per out-event: " + ((double)latencyPerEventSummed/(double)numberOfEvents) + " ms");
	}
	
}
