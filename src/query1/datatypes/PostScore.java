package query1.datatypes;

/**
 * contains everything needed to tranfer the current post score in an update form the worker to the merger
 * 
 * the variables represent the state of the post score when the update was done; it is a "snapshot" from exactly that point in time
 * 
 * @author mayerrn
 *
 */
public class PostScore {

	private int totalScore;
	private int numberOfCommenters;
		
	public PostScore(int totalScore, int numberOfCommenters){
		this.totalScore = totalScore;
		this.numberOfCommenters = numberOfCommenters;
	}
	
	public int getTotalScore() {
		return totalScore;
	}
	public void setTotalScore(int totalScore) {
		this.totalScore = totalScore;
	}
	/**
	 * increment the total score by given number
	 */
	public void incrementTotalScore(int increment){
		totalScore += increment;
	}
	
	public int getNumberOfCommenters() {
		return numberOfCommenters;
	}
	public void setNumberOfCommenters(int numberOfCommenters) {
		this.numberOfCommenters = numberOfCommenters;
	}
}
