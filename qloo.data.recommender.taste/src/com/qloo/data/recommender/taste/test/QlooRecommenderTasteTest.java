package com.qloo.data.recommender.taste.test;

import java.util.Map;
import java.util.UUID;


import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.qloo.data.recommender.taste.QlooRecommenderTaste;


public class QlooRecommenderTasteTest {
	static QlooRecommenderTaste qr;
	
	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		qr = QlooRecommenderTaste.getInstance("/Users/qloo/work/test2.out");
		
		qr.init("/Users/qloo/work/test2.csv", 15, 0.7f, 30);
	}
	
	//@Ignore("Not Ready to Run")
	@Test
	public void printUserSimilarityMap() {
		System.out.println("@Test - printUserSimilarityMap");
		
		Map<UUID, Double> map = qr.getUserSimilarityMap(UUID.fromString("b55216ae-3fc8-4bb9-80db-29d2c8cc151a"));
		
		System.out.println(map.size());
		
		for (Map.Entry<UUID, Double> entry : map.entrySet()) {
			System.out.println("score: " + entry.getValue() + "\tuid: " + entry.getKey());
		}
	}
}
