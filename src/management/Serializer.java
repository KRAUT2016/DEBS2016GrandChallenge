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

/**
 * the serializer connects the incoming event streams to the CEP system
 * and provides the sequence of events in the incoming queues of the 
 * query processors (query1 and query2)
 * @author mayerrn
 *
 */
public class Serializer implements Runnable {
	
	final private Time time = new Time();
	private static final char del = Constants.VALUE_SEPARATOR;
	private String commentPath;
	private String postPath;
	private String friendshipPath;
	private String likePath;
	
	
	
	/**
	 * this array contains the first event from each incoming queue
	 * (friendships, posts, comments, likes)
	 * The serializer decides which event to take next, based on 
	 * comparison of the first events, and also puts the next 
	 * first event into the queue
	 */
	private static AbstractEvent[] firstEvents = new AbstractEvent[4];
	
	
	/**
	 * the incoming queues for the different event streams from the sources
	 */
//	public static ArrayBlockingQueue<AbstractEvent> friendshipsInQueue = new ArrayBlockingQueue<AbstractEvent>(1);
//	public static ArrayBlockingQueue<AbstractEvent> postsInQueue = new ArrayBlockingQueue<AbstractEvent>(5);
//	public static ArrayBlockingQueue<AbstractEvent> commentsInQueue = new ArrayBlockingQueue<AbstractEvent>(10);
//	public static ArrayBlockingQueue<AbstractEvent> likesInQueue = new ArrayBlockingQueue<AbstractEvent>(5);
	
	/**
	 * the incoming queues are kept in an array
	 */
	@SuppressWarnings("unchecked")
	public static ArrayBlockingQueue<AbstractEvent>[] incomingQueues = (ArrayBlockingQueue<AbstractEvent>[]) new ArrayBlockingQueue<?>[4];
	
	/**
	 * the inQueue of query1 processing / outqueue1 of serializer
	 * 
	 * event sequence of type Post and Comment is put here
	 */
	public static ArrayBlockingQueue<AbstractEvent> query1Queue = new ArrayBlockingQueue<AbstractEvent>(Constants.Q1SIZE);
	
	/**
	 * the inQueue of query2 processing / outqueue2 of serializer
	 * 
	 * event sequence of type Friendship, Comment and Like is put here
	 */
	public static ArrayBlockingQueue<AbstractEvent> query2Queue = new ArrayBlockingQueue<AbstractEvent>(Constants.Q2SIZE);

	
	public Serializer(){
		
	}
	
	public Serializer(String pathComment, String pathFriendship, String pathLike, String pathPost){
		this.commentPath = pathComment;
		this.friendshipPath = pathFriendship;
		this.likePath = pathLike;
		this.postPath = pathPost;
	}
	
	
	/**
	 * the run method of the thread
	 * 
	 * continuously takes events from the 4 incoming queues and puts them into a well-defined order
	 * according to their timestamp
	 */
	@Override
	public void run() {
		BufferedReader friendshipReader = null;
		BufferedReader likeReader = null;
		BufferedReader postReader = null;
		BufferedReader commentReader = null;
		String absolutepath = new File("").getAbsolutePath() + File.separatorChar;
		String in = ""; // read string
		
		try {
			friendshipReader = new BufferedReader(new InputStreamReader(new FileInputStream(absolutepath + this.friendshipPath), "UTF-8"));
			likeReader = new BufferedReader(new InputStreamReader(new FileInputStream(absolutepath + this.likePath), "UTF-8"));
			postReader = new BufferedReader(new InputStreamReader(new FileInputStream(absolutepath + this.postPath), "UTF-8"));
			commentReader = new BufferedReader(new InputStreamReader(new FileInputStream(absolutepath + this.commentPath), "UTF-8"));
		}
		catch (Exception e){
			e.printStackTrace();
		}
		
//		this.initializeFirstEventsArray();
				
		/*
		 * initializing first events array
		 */
		
			try {
				firstEvents[0] = this.buildFriendship(friendshipReader.readLine());
				firstEvents[1] = this.buildPost(postReader.readLine());
				firstEvents[2] = this.buildComment(commentReader.readLine());
				firstEvents[3] = this.buildLike(likeReader.readLine());
					
			} catch (ParseException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		
		runloop:
		while(true){
						
			long lowestTimestamp = Long.MAX_VALUE;
			byte indexOfNextEvent = 0;
			
			// check which of the firstEvents has the lowest timestamp
			for (byte i = 0; i < 4; i++){
				if (firstEvents[i].getTs() < lowestTimestamp){
					indexOfNextEvent = i;
					lowestTimestamp = firstEvents[i].getTs();
				}
			}

//			System.out.println(query1Queue.size() + ", " + firstEvents[indexOfNextEvent].getType());
			
//			Thread.yield();
			/*
			 * put the next event into the queues, according to event type
			 */
			try{
				
				// depending on the event type, the event has to be put into different queues			
				switch (firstEvents[indexOfNextEvent].getType()){					
					case FRIENDSHIP:  // friendship
						query2Queue.put(firstEvents[0]);
						
						if ((in = friendshipReader.readLine()) != null){
							firstEvents[0] = this.buildFriendship(in);
						}
						else{
							firstEvents[0] = new StreamClosed();
						}
						break;
						
					case POST:  // post
						query1Queue.put(firstEvents[1]);
												
						if ((in = postReader.readLine()) != null){
							firstEvents[1] = this.buildPost(in);
						}
						else{
							firstEvents[1] = new StreamClosed();
						}
						break;
						
					case COMMENT:  // comment
						query1Queue.put(firstEvents[2]);
						query2Queue.put(firstEvents[2]);
						
						if ((in = commentReader.readLine()) != null){
							firstEvents[2] = this.buildComment(in);
						}
						else{
							firstEvents[2] = new StreamClosed();
						}
						break;
						
					case LIKE:  // like
						query2Queue.put(firstEvents[3]); 
						
						if ((in = likeReader.readLine()) != null){
							firstEvents[3] = this.buildLike(in);
						}
						else{
							firstEvents[3] = new StreamClosed();
						}
						break;
					case STREAM_CLOSED: 
						query1Queue.put(firstEvents[indexOfNextEvent]); query2Queue.put(firstEvents[indexOfNextEvent]); 
						break runloop; // serializer shut down
				}
				
				
			}catch (Exception e){
				e.printStackTrace();
			}
				
		}		
//		System.out.println("Serializer shut down.");
	}
		
	
	/**
	 * 
	 * @param commentString consists of 7 separated fields
	 * @throws ParseException 
	 */
	private Comment buildComment(String commentString) throws ParseException{		
		long creationTime = System.nanoTime();
		String[] parameters = fastSplit(commentString.substring(29));
			
		// build the parameter values
		String ts_string = commentString.substring(0,29);
		long ts = time.timeStamp(ts_string);
			
		long comment_id = new Long(parameters[0]).longValue(); 
		long user_id = new Long(parameters[1]).longValue();
		String comment = parameters[2]; 
		String user = parameters[3]; 
		long comment_replied = (parameters[4].equals("")) ? -1 : new Long(parameters[4]).longValue(); 
		long post_commented = (parameters[5].equals("")) ? -1 : new Long(parameters[5]).longValue();
			
		// build the object and hand it to the Serializer	
		return new Comment(ts, comment_id, user_id, comment, user, comment_replied, post_commented, creationTime);
	}
	
	/**
	 * 
	 * @param friendshipString consists of 3 separated fields
	 * @throws ParseException 
	 */
	private Friendship buildFriendship(String friendshipString) throws ParseException{
		long creationTime = System.nanoTime();
		String[] parameters = fastSplit(friendshipString.substring(29)); 
		
		// build the parameter values
		String ts_string = friendshipString.substring(0,29);
		long ts = time.timeStamp(ts_string);
		long user_id_1 = new Long(parameters[0]).longValue(); // second field: user_id_1
		long user_id_2 = new Long(parameters[1]).longValue();// third field: user_id_2
						
		// build the object and hand it to the Serializer
		return new Friendship(ts, user_id_1, user_id_2, creationTime);		
	}
	
	/**
	 * 
	 * @param likeString consists of 3 separated fields
	 * @throws ParseException 
	 */
	private Like buildLike(String likeString) throws ParseException{
	
		long creationTime = System.nanoTime();
		String[] parameters = fastSplit(likeString.substring(29));
		
		// build the parameter values
		String ts_string = likeString.substring(0,29);
		long ts = time.timeStamp(ts_string);
		long user_id = new Long(parameters[0]).longValue(); // second field: user_id
		long comment_id = new Long(parameters[1]).longValue();// third field: comment_id
						
		// build the object and hand it to the Serializer
		return new Like(ts, user_id, comment_id, creationTime);				
	}
	
	/**
	 * 
	 * @param postString consists of 5 separated fields
	 * @throws ParseException 
	 */
	private Post buildPost(String postString) throws ParseException{
				
		long creationTime = System.nanoTime();
		String[] parameters = fastSplit(postString.substring(29));
		
		// build the parameter values
		String ts_string = postString.substring(0,29);
		long ts =  time.timeStamp(ts_string);
		long post_id = new Long(parameters[0]).longValue(); // second field: post_id
		long user_id = new Long(parameters[1]).longValue();// third field: user_id
		 // forth field: POST IS NOT USED
		String user = parameters[3]; // fifth field: user (= his user name)
			
		// build the object and hand it to the Serializer
		return new Post(ts, post_id, user_id, user, creationTime);	
		
	}
	
	/**
	 * a faster splitting implementation 
	 * @param s string to be split
	 * @return
	 */
	private String[] fastSplit(String s) {

		int c = 1;
		for (int i = 0; i < s.length(); i++)
			if (s.charAt(i) == del)
				c++;
		
		String[] result = new String[c];
		int a = -1;
		int b = 0;

		for (int i = 0; i < c; i++) {

			while (b < s.length() && s.charAt(b) != del)
				b++;
			result[i] = s.substring(a+1, b);
			a = b;
			b++;		
		}

		return result;
	}
	
}
