package com.qloo.data.test.graph.netflix;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.qloo.data.graph.netflix.ChoiceNode;
import com.qloo.data.graph.netflix.QlooGraphDAO3;
import com.qloo.data.graph.netflix.UserNode;


public class QlooGraphDAO3Test {
	static QlooGraphDAO3 qgd3;

	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		qgd3 = new QlooGraphDAO3();
    }
	
	//@Ignore("Not Ready to Run")
	@Test
	public void input() {
		System.out.println("@Test - input");
		
		try {
			qgd3.input("/Users/qloo/work/test3.out");
	    } catch (IOException ioe) {
	    	ioe.printStackTrace();
	    	return;
	    }
	}

	//@Ignore("Not Ready to Run")
	@Test
	public void printSimilarUsers() {
		System.out.println("@Test - printSimilarUsers");
		
		HashSet<UserNode> neighborSet = new HashSet<UserNode>();
		
		long lStart = System.currentTimeMillis();
		
	    List<ChoiceNode> likeList = qgd3.getChoiceNodeLikeList(UserNode.newUserNode(UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce0")));

	    for (ChoiceNode cn : likeList) {
	    	List<UserNode> likedList = qgd3.getUserNodeLikedList(cn);
	    	neighborSet.addAll(likedList);
	    }
	    
	    List<ChoiceNode> dislikeList = qgd3.getChoiceNodeDislikeList(UserNode.newUserNode(UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce0")));

	    for (ChoiceNode cn : dislikeList) {
	    	List<UserNode> dislikedList = qgd3.getUserNodeDislikedList(cn);
	    	neighborSet.addAll(dislikedList);
	    }
	   
	    System.out.println("like count: " + likeList.size() + "\tdislike count: " + dislikeList.size() + " unique user count: " + neighborSet.size() + " takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
	}
	
	@Test
	public void printPopularLHM() {
		System.out.println("@Test - printPopularLHM");
		
		for (int i = 0; i < qgd3.getPopularLHMArray().length; i++) {
			for (Map.Entry<Short, LinkedHashMap<Integer, Double>> entry : qgd3.getPopularLHMArray()[i].entrySet()) {
				LinkedHashMap<Integer, Double> lhm = entry.getValue();
				
				System.out.print("giid: " + i + "\ttiid: " + entry.getKey() + "\tsize: " + lhm.size());
				
				for (Map.Entry<Integer, Double> entry2 : lhm.entrySet()) {
					System.out.print("\t" + entry2.getValue());
				}
				
				System.out.println();
			}
		}
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void printFollows() {
		System.out.println("@Test - printFollows");
		
	    List<UserNode> followList = qgd3.getUserNodeFollowList(UserNode.newUserNode(UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce0")));
	    
	    long lStart = System.currentTimeMillis();
	    
	    int count = 0;
	    for (UserNode un : followList) {
	    	List<UserNode> followedList = qgd3.getUserNodeFollowedList(un);
	    	count += followedList.size();
	    }
	   
	    System.out.println("follow count: " + followList.size() + " total followed count: " + count + " takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
	}
}
