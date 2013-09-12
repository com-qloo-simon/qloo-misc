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

import com.qloo.data.graph.netflix.GeoChoiceNode;
import com.qloo.data.graph.netflix.ChoiceNode;
import com.qloo.data.graph.netflix.QlooGraphDAO4;
import com.qloo.data.graph.netflix.UserNode;


public class QlooGraphDAO4Test {
	static QlooGraphDAO4 qgd4;
	
	static UUID uid;
	

	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		qgd4 = new QlooGraphDAO4();
		
		try {
			qgd4.input("/Users/qloo/work/test4.out");
	    } catch (IOException ioe) {
	    	ioe.printStackTrace();
	    	return;
	    }
		
		// uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2");
		uid = UUID.fromString("e32b8d38-7674-4bde-959a-003f05c135bc".toUpperCase());
    }

	//@Ignore("Not Ready to Run")
	@Test
	public void printSimilarUsers() {
		System.out.println("@Test - printSimilarUsers");
		
		HashSet<UserNode> neighborSet = new HashSet<UserNode>();
		
		long lStart = System.currentTimeMillis();
		
	    List<ChoiceNode> likeList = qgd4.getChoiceNodeLikeList(UserNode.newUserNode(uid));

	    for (ChoiceNode cn : likeList) {
	    	List<UserNode> likedList = qgd4.getUserNodeLikedList(cn);
	    	neighborSet.addAll(likedList);
	    }
	    
	    List<ChoiceNode> dislikeList = qgd4.getChoiceNodeDislikeList(UserNode.newUserNode(uid));

	    for (ChoiceNode cn : dislikeList) {
	    	List<UserNode> dislikedList = qgd4.getUserNodeDislikedList(cn);
	    	neighborSet.addAll(dislikedList);
	    }
	   
	    System.out.println("like count: " + likeList.size() + "\tdislike count: " + dislikeList.size() + " unique user count: " + neighborSet.size() + " takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void printPopularLHM() {
		System.out.println("@Test - printPopularLHM");
		
		for (int i = 0; i < qgd4.getPopularLHMArray().length; i++) {
			for (Map.Entry<Short, LinkedHashMap<Integer, Double>> entry : qgd4.getPopularLHMArray()[i].entrySet()) {
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
		
	    List<UserNode> followList = qgd4.getUserNodeFollowList(UserNode.newUserNode(UUID.fromString("d66ff676-a885-4fd3-b296-ec565ed13a5f")));
	    
	    long lStart = System.currentTimeMillis();
	    
	    int count = 0;
	    for (UserNode un : followList) {
	    	List<UserNode> followedList = qgd4.getUserNodeFollowedList(un);
	    	count += followedList.size();
	    }
	   
	    System.out.println("follow count: " + followList.size() + " total followed count: " + count + " takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void printLikeList() {
		System.out.println("@Test - printLikeList");
		
		List<ChoiceNode> cnList = qgd4.getChoiceNodeLikeList(UserNode.newUserNode(UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce0")));
		
		for (ChoiceNode cn : cnList) {
			if (cn instanceof GeoChoiceNode) {
				GeoChoiceNode gcn = (GeoChoiceNode)cn;
				System.out.println("cid: " + gcn.cid + "\tcity id: " + gcn.cityId + "\tlat: " + gcn.lat + "\tlon: " + gcn.lon);
			}
		}
	
	}
}
