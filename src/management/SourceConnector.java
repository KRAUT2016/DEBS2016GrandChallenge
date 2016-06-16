package management;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import common.events.Comment;
import common.events.EventType;
import common.events.Friendship;
import common.events.Like;
import common.events.Post;
import common.events.StreamClosed;
import util.Constants;
import util.Time;

/**
 * connects to a source file and reads events from it
 * 
 * marshals the events and puts them into the corresponding inqueue of the 
 * Serializer
 * @author mayerrn
 *
 */
public class SourceConnector implements Runnable {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
	
//	private EventType type; // the type of events handled by the sourceConnector
//	private String filePath;
//	private Time time;
//	private static final char del = Constants.VALUE_SEPARATOR;
//	
//	public SourceConnector(EventType type, String filename){
//		this.type = type;
//		filePath = filename;
//		time = new Time();
//	}
//	
//		
//	/**
//	 * 
//	 * @param commentString consists of 7 separated fields
//	 */
//	private void buildComment(String commentString){		
//		long creationTime = System.nanoTime();
//		String[] parameters = fastSplit(commentString.substring(29));
//		
//		try {
//			// build the parameter values
//			long ts = time.timeStamp(commentString.substring(0,28));
//			
//			long comment_id = new Long(parameters[0]).longValue(); 
//			long user_id = new Long(parameters[1]).longValue();
//			String comment = parameters[2]; 
//			String user = parameters[3]; 
//			long comment_replied = (parameters[4].equals("")) ? -1 : new Long(parameters[4]).longValue(); 
//			long post_commented = (parameters[5].equals("")) ? -1 : new Long(parameters[5]).longValue();
//			
//			// build the object and hand it to the Serializer	
//			Serializer.commentsInQueue.put(new Comment(ts, comment_id, user_id, comment, user, comment_replied, post_commented, creationTime));
//		} catch (ParseException e) {
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		} 
//	}
//	
//	/**
//	 * 
//	 * @param friendshipString consists of 3 separated fields
//	 */
//	private void buildFriendship(String friendshipString){
//		long creationTime = System.nanoTime();
//		String[] parameters = fastSplit(friendshipString.substring(29)); 
//		
//		try {
//			// build the parameter values
//			long ts = time.timeStamp(friendshipString.substring(0,28));
//			long user_id_1 = new Long(parameters[0]).longValue(); // second field: user_id_1
//			long user_id_2 = new Long(parameters[1]).longValue();// third field: user_id_2
//						
//			// build the object and hand it to the Serializer
//			Friendship friendshipObject = new Friendship(ts, user_id_1, user_id_2, creationTime);	
//			Serializer.friendshipsInQueue.put(friendshipObject);
//		} catch (ParseException e) {
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	/**
//	 * 
//	 * @param likeString consists of 3 separated fields
//	 */
//	private void buildLike(String likeString){
//		long creationTime = System.nanoTime();
//		String[] parameters = fastSplit(likeString.substring(29));
//		
//		try {
//			// build the parameter values
//			long ts = time.timeStamp(likeString.substring(0,28));
//			long user_id = new Long(parameters[0]).longValue(); // second field: user_id
//			long comment_id = new Long(parameters[1]).longValue();// third field: comment_id
//						
//			// build the object and hand it to the Serializer
//			Like likeObject = new Like(ts, user_id, comment_id, creationTime);
//			Serializer.likesInQueue.put(likeObject);
//		} catch (ParseException e) {
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	/**
//	 * 
//	 * @param postString consists of 5 separated fields
//	 */
//	private void buildPost(String postString){
//		long creationTime = System.nanoTime();
//		String[] parameters = fastSplit(postString.substring(29));
//		
//		try {
//			// build the parameter values
//			long ts =  time.timeStamp(postString.substring(0,28));
//			long post_id = new Long(parameters[0]).longValue(); // second field: post_id
//			long user_id = new Long(parameters[1]).longValue();// third field: user_id
//			 // forth field: POST IS NOT USED
//			String user = parameters[3]; // fifth field: user (= his user name)
//			
//			// build the object and hand it to the Serializer
//			Post postObject = new Post(ts, post_id, user_id, user, creationTime);	
//			Serializer.postsInQueue.put(postObject);
//		} catch (ParseException e) {
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	/**
//	 * a faster splitting implementation 
//	 * @param s string to be split
//	 * @return
//	 */
//	private String[] fastSplit(String s) {
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
//	
//		
//	@Override
//	public void run() {
//		try {
//			Stream<String> lines = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8);
//			switch (this.type){
//				case COMMENT: 	 	lines.forEachOrdered(line -> this.buildComment(line)); break;
//				case FRIENDSHIP:	lines.forEachOrdered(line -> this.buildFriendship(line)); break;
//				case LIKE: 			lines.forEachOrdered(line -> this.buildLike(line)); break;
//				case POST: 			lines.forEachOrdered(line -> this.buildPost(line)); break;
//			}
//			lines.close();
//			StreamClosed closeEvent = new StreamClosed();	
//			switch (this.type){
//				case COMMENT: 	 	Serializer.commentsInQueue.put(closeEvent); break;
//				case FRIENDSHIP:	Serializer.friendshipsInQueue.put(closeEvent); break;
//				case LIKE: 			Serializer.likesInQueue.put(closeEvent); break;
//				case POST: 			Serializer.postsInQueue.put(closeEvent); break;
//			}		
////			System.out.println("SourceConnector " + this.type + " shut down.");
//			
//		} catch (IOException e) {
//			e.printStackTrace();
//		} 
//		catch (InterruptedException e){
//			e.printStackTrace();
//		}
//				
//	}
}
