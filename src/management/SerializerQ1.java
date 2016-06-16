package management;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import common.events.AbstractEvent;
import common.events.Comment;
import common.events.Friendship;
import common.events.Like;
import common.events.Post;
import common.events.StreamClosed;
import util.Constants;
import util.Time;
import util.Util;

/**
 * the serializer connects the incoming event streams to the CEP system
 * and provides the sequence of events in the incoming queues of the 
 * query processors (query1 and query2)
 * @author mayerrn
 *
 */
public class SerializerQ1 {
	
	final private Time time = new Time();
	private static final char del = Constants.VALUE_SEPARATOR;
	private String commentPath;
	private String postPath;
	private String friendshipPath;
	private String likePath;
	
	private BufferedReader friendshipReader = null;
	private BufferedReader likeReader = null;
	private BufferedReader postReader = null;
	private BufferedReader commentReader = null;
	
	
	/**
	 * this array contains the first event from each incoming queue
	 * (friendships, posts, comments, likes)
	 * The serializer decides which event to take next, based on 
	 * comparison of the first events, and also puts the next 
	 * first event into the queue
	 */
	private static AbstractEvent[] firstEvents = new AbstractEvent[2];
	
	public SerializerQ1(String pathComment, String pathFriendship, String pathLike, String pathPost){
		this.commentPath = pathComment;
//		this.friendshipPath = pathFriendship;
//		this.likePath = pathLike;
		this.postPath = pathPost;

		String absolutepath = new File("").getAbsolutePath() + File.separatorChar;
		
		try {
//			friendshipReader = new BufferedReader(new InputStreamReader(new FileInputStream(absolutepath + this.friendshipPath), "UTF-8"));
//			likeReader = new BufferedReader(new InputStreamReader(new FileInputStream(absolutepath + this.likePath), "UTF-8"));
			postReader = new BufferedReader(new InputStreamReader(new FileInputStream(absolutepath + this.postPath), "UTF-8"), Constants.BUFFERING_IN_READERS);
			commentReader = new BufferedReader(new InputStreamReader(new FileInputStream(absolutepath + this.commentPath), "UTF-8"), Constants.BUFFERING_IN_READERS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {

			String postString = postReader.readLine();
			if (postString != null && !postString.isEmpty()){
				firstEvents[0] = this.buildPost(postString);	
			}
			else {
				firstEvents[0] = new StreamClosed();
			}
			
			String commentString = commentReader.readLine();
			if (commentString != null && !commentString.isEmpty()){
				firstEvents[1] = this.buildComment(commentString);
			}
			else {
				firstEvents[1] = new StreamClosed();
			}

		} catch (ParseException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
	
	
	/**
	 * the run method of the thread
	 * 
	 * continuously takes events from the 4 incoming queues and puts them into a well-defined order
	 * according to their timestamp
	 */
	public AbstractEvent getNextEvent() {
					
		long lowestTimestamp = Long.MAX_VALUE;
		byte indexOfNextEvent = 0;
		AbstractEvent nextEve = null;
		String in = ""; // read string
		
		// check which of the firstEvents has the lowest timestamp
		for (byte i = 0; i < 2; i++){
			if (firstEvents[i].getTs() < lowestTimestamp){
				indexOfNextEvent = i;
				lowestTimestamp = firstEvents[i].getTs();
			}
		}

		/*
		 * put the next event into the queues, according to event type
		 */
		try{

			// depending on the event type, the event has to be put into different queues			
			switch (firstEvents[indexOfNextEvent].getType()){					
//			case FRIENDSHIP:  // friendship
//				nextEve = firstEvents[0];
//
//				if ((in = friendshipReader.readLine()) != null){
//					firstEvents[0] = this.buildFriendship(in);
//				}
//				else{
//					firstEvents[0] = new StreamClosed();
//				}
//				break;

			case POST:  // post
				nextEve = firstEvents[0];
//				query1Queue.put(firstEvents[1]);

				if ((in = postReader.readLine()) != null && !in.isEmpty()){
					firstEvents[0] = this.buildPost(in);
				}
				else{
					firstEvents[0] = new StreamClosed();
				}
				break;

			case COMMENT:  // comment
				nextEve = firstEvents[1];
//				query2Queue.put(firstEvents[2]);

				if ((in = commentReader.readLine()) != null && !in.isEmpty()){
					firstEvents[1] = this.buildComment(in);
				}
				else{
					firstEvents[1] = new StreamClosed();
				}
				break;

//			case LIKE:  // like
//				nextEve = firstEvents[2]; 
//
//				if ((in = likeReader.readLine()) != null){
//					firstEvents[2] = this.buildLike(in);
//				}
//				else{
//					firstEvents[2] = new StreamClosed();
//				}
//				break;
			case STREAM_CLOSED: 
				nextEve = firstEvents[indexOfNextEvent];
//				query2Queue.put(firstEvents[indexOfNextEvent]); 
				break; // serializer shut down
			default:
				nextEve = null;
				break;
			}

		}catch (Exception e){
			e.printStackTrace();
		}
		return nextEve;
	}
		
	
	/**
	 * 
	 * @param commentString consists of 7 separated fields
	 * @throws ParseException 
	 */
	private Comment buildComment(String commentString) throws ParseException{		
		long creationTime = System.nanoTime();
		String[] parameters = Util.fastCommentSplit(commentString.substring(29),6,del);
			
		// build the parameter values
		String ts_string = commentString.substring(0,29);
		long ts = time.timeStamp(ts_string);
			
		long comment_id = new Long(parameters[0]).longValue(); 
		long user_id = new Long(parameters[1]).longValue();
//		String comment = parameters[2];  comment is not used
		String user = parameters[3]; 
		long comment_replied = (parameters[4].equals("")) ? -1 : new Long(parameters[4]).longValue(); 
		long post_commented = (parameters[5].equals("")) ? -1 : new Long(parameters[5]).longValue();
			
		// build the object and hand it to the Serializer	
		return new Comment(ts, comment_id, user_id, "", user, comment_replied, post_commented, creationTime);
	}

	
	/**
	 * 
	 * @param postString consists of 5 separated fields
	 * @throws ParseException 
	 */
	private Post buildPost(String postString) throws ParseException{
				
		long creationTime = System.nanoTime();
		String[] parameters = Util.fastPostSplit(postString.substring(29),4,del);
		
		// build the parameter values
		String ts_string = postString.substring(0,29);
		long ts =  time.timeStamp(ts_string);
		long post_id = new Long(parameters[0]).longValue(); // second field: post_id
		long user_id = new Long(parameters[1]).longValue();// third field: user_id
		 // third field: POST IS NOT USED
		String user = parameters[3]; // fifth field: user (= his user name)
			
		// build the object and hand it to the Serializer
		return new Post(ts, post_id, user_id, user, creationTime);		
	}
	

	
//	/**
//	 * a faster splitting implementation 
//	 * @param s string to be split
//	 * @return
//	 */
//	private String[] fastSplit2(String s) {
//
//		int c = 1;
//		for (int i = 0; i < s.length(); i++)
//			if (s.charAt(i) == del)
//				c++;
//		
//		String[] result = new String[c];
//		int a = -1;
//		int b = 0;
//
//		for (int i = 0; i < c; i++) {
//
//			while (b < s.length() && s.charAt(b) != del)
//				b++;
//			result[i] = s.substring(a+1, b);
//			a = b;
//			b++;		
//		}
//
//		return result;
//	}
	
}
