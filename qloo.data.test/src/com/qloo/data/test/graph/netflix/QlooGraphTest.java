package com.qloo.data.test.graph.netflix;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import com.qloo.data.graph.netflix.QlooGraph;
import com.qloo.data.graph.netflix.UserNode;
import com.qloo.data.graph.netflix.ChoiceNode;


public class QlooGraphTest {
	static QlooGraph qg;
	
	static UUID uid;
	static UserNode un;

	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		qg = QlooGraph.getInstance("/Users/qloo/work/test.out");
		
		uid = UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce0");
		
		un = qg.getUserNode(uid);
    }
    
	//@Ignore("Not Ready to Run")
	@Test
	public void printUserInfo() {
		System.out.println("@Test - printUserInfo");
		
		System.out.println("Graph: gender: " + un.gender + "\tage: " + un.age);
	}
	
	//@Ignore("Not Ready to Run")
	@Test
	public void printLikeList() {
		System.out.println("@Test - printLikeList");
		
		List<ChoiceNode> cnList = qg.getChoiceNodeLikeList(un);
		
		System.out.println("like list size: " + cnList.size());
	}

	//@Ignore("Not Ready to Run")
	@Test
	public void printSortedSimilarUsers_basic() {
		System.out.println("@Test - printSortedSimilarUsers_basic");
		
		long lStart = System.currentTimeMillis();
		
		LinkedHashMap<UserNode, Integer> lhm = qg.getUserNodeSimilarMap(un, (short)30);
		
		System.out.println("printSortedSimilarUsers takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
		
		for (Map.Entry<UserNode, Integer> entry : lhm.entrySet()) {
			System.out.println("score: " + entry.getValue() + "\tuid: " + entry.getKey().uid);
		}
	}
	
	//@Ignore("Not Ready to Run")
	@Test
	public void printSortedChoiceNodeRecommenderMap_basic() {
		System.out.println("@Test - printSortedChoiceNodeRecommenderMap_basic");
		
		long lStart = System.currentTimeMillis();
		
		LinkedHashMap<ChoiceNode, Integer> lhm = qg.getChoiceNodeRecommendationMap(un, (short)30);
		
		System.out.println("getSortedChoiceNodeRecommenderMap takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
		
		System.out.println("size: " + lhm.size());
		
		int count = 0;
		for (Map.Entry<ChoiceNode, Integer> entry : lhm.entrySet()) {
			System.out.println("score: " + entry.getValue() + "\tcid: " + entry.getKey().cid);
			
			count++; if (count >= 250) break;
		}
	}
}
