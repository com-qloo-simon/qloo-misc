package com.qloo.data.test.graph.netflix;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.qloo.data.graph.netflix.ChoiceNode;
import com.qloo.data.graph.netflix.QlooGraphDAO2;
import com.qloo.data.graph.netflix.UserNode;


public class QlooGraphDAO2Test {
	static QlooGraphDAO2 qgd2;
	
	static UUID uid;
	

	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		qgd2 = new QlooGraphDAO2();
		
		try {
			qgd2.input("/Users/qloo/work/test2.out");
	    } catch (IOException ioe) {
	    	ioe.printStackTrace();
	    	return;
	    }
		
		// uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2");
		uid = UUID.fromString("e32b8d38-7674-4bde-959a-003f05c135bc");
    }

	//@Ignore("Not Ready to Run")
	@Test
	public void printSimilarUsers() {
		System.out.println("@Test - printSimilarUsers");
		
	    List<ChoiceNode> likeList = qgd2.getChoiceNodeLikeList(UserNode.newUserNode(uid));
	    
	    System.out.println("likeList size: " + likeList.size());
	    
	    long lStart = System.currentTimeMillis();
	    
	    HashSet<UserNode> neighborSet = new HashSet<UserNode>();
	    for (ChoiceNode cn : likeList) {
	    	List<UserNode> likedList = qgd2.getUserNodeLikedList(cn);
	    	neighborSet.addAll(likedList);
	    }
	   
	    System.out.println("matched choice count: " + likeList.size() + " unique user count: " + neighborSet.size() + " takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void printPopularLHM() {
		System.out.println("@Test - printPopularLHM");
		
		for (int i = 0; i < qgd2.getPopularLHMArray().length; i++) {
			for (Map.Entry<Short, LinkedHashMap<Integer, Double>> entry : qgd2.getPopularLHMArray()[i].entrySet()) {
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
		
	    List<UserNode> followList = qgd2.getUserNodeFollowList(UserNode.newUserNode(UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2")));
	    
	    long lStart = System.currentTimeMillis();
	    
	    int count = 0;
	    for (UserNode un : followList) {
	    	List<UserNode> followedList = qgd2.getUserNodeFollowedList(un);
	    	count += followedList.size();
	    }
	   
	    System.out.println("follow count: " + followList.size() + " total followed count: " + count + " takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
	}
}
