package com.qloo.data.test.graph.netflix;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.qloo.data.graph.netflix.ChoiceNode;
import com.qloo.data.graph.netflix.QlooGraphDAO;
import com.qloo.data.graph.netflix.UserNode;


public class QlooGraphDAOTest {
	static QlooGraphDAO qgd;

	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		qgd = new QlooGraphDAO();
		
		try {
			qgd.input("/Users/qloo/work/test.out");
	    } catch (IOException ioe) {
	    	ioe.printStackTrace();
	    	return;
	    }
    }
	
	//@Ignore("Not Ready to Run")
	@Test
	public void printSimilarUsers() {
		System.out.println("@Test - printSimilarUsers");
		
	    List<ChoiceNode> choices = qgd.getChoiceNodeLikeList(UserNode.newUserNode(UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2")));
	    //List<UUID> choices = qg.getAllLikes(UUID.fromString("6f5b69bb-46a7-4a9a-8d25-0934133c9a7a"));
	    
	    long lStart = System.currentTimeMillis();
	    
	    int count = 0;
	    for (ChoiceNode cn : choices) {
	    	List<UserNode> users = qgd.getUserNodeLikedList(cn);
	    	count += users.size();
	    }
	   
	    System.out.println("matched choice count: " + choices.size() + " user count: " + count + " takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
	}
	
	//@Ignore("Not Ready to Run")
	@Test
	public void printFollows() {
		System.out.println("@Test - printFollows");
		
	    List<UserNode> follows = qgd.getUserNodeFollowList(UserNode.newUserNode(UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2")));
	    
	    long lStart = System.currentTimeMillis();
	    
	    int count = 0;
	    for (UserNode un : follows) {
	    	List<UserNode> followeds = qgd.getUserNodeFollowedList(un);
	    	count += followeds.size();
	    }
	   
	    System.out.println("follow count: " + follows.size() + " total followed count: " + count + " takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
	}
}
