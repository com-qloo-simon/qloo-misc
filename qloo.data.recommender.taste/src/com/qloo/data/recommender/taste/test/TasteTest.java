package com.qloo.data.recommender.taste.test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TasteTest {
	static DataModel model;
	
	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		long lStart = System.currentTimeMillis();
		
		try {
			model = new GenericBooleanPrefDataModel(new FileDataModel(new File("/Users/qloo/work/test.csv")));
		} catch (TasteException te) {
			te.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (NoSuchElementException nse) {
			nse.printStackTrace();
		}
		
		System.out.println("setup takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
	}
	
	//@Ignore("Not Ready to Run")
	@Test
	public void testGenericBooleanPrefUserBasedRecommender() {
		System.out.println("@Test - testGenericBooleanPrefUserBasedRecommender");
		
		long lStart = System.currentTimeMillis();
		
		try {			
			//UserSimilarity userSimilarity = new PearsonCorrelationSimilarity(model);
			UserSimilarity userSimilarity = new LogLikelihoodSimilarity(model);
			
			//userSimilarity.setPreferenceInferrer(new AveragingPreferenceInferrer(model));
			
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(10, 0.7, userSimilarity, model);
			
			long[] nbs = neighborhood.getUserNeighborhood(1);
			System.out.println("neighborhood count: " + nbs.length);
			
			GenericBooleanPrefUserBasedRecommender recommender = new GenericBooleanPrefUserBasedRecommender(model, neighborhood, userSimilarity);
			
			Recommender cachingRecommender = new CachingRecommender(recommender);
			
			long lEnd = System.currentTimeMillis();
			System.out.println("building takes " + (lEnd - lStart) + " mill seconds");
			lStart = lEnd;
			
			List<RecommendedItem> recommendations = cachingRecommender.recommend(1, 30);
			
			System.out.println("recommendation takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
			
			System.out.println(recommendations.size());
			
			for (RecommendedItem item : recommendations) {
			//	System.out.println(item);
			}
		} catch (TasteException te) {
			te.printStackTrace();
		} catch (Throwable th) {
			th.printStackTrace();
		}
	}
	
	// item-based recommender builds very fast, 3 times slow at first recommendation, OK at second recommendation
	@Ignore("Not Ready to Run")
	@Test
	public void testGenericBooleanPrefItemBasedRecommender() {
		System.out.println("@Test - testGenericBooleanPrefItemBasedRecommender");
		
		long lStart = System.currentTimeMillis();
		
		try {	
			ItemSimilarity itemSimilarity = new PearsonCorrelationSimilarity(model);
			
			Recommender recommender = new GenericItemBasedRecommender(model, itemSimilarity);
			
			Recommender cachingRecommender = new CachingRecommender(recommender);
			
			System.out.println("building takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
			
			
			lStart = System.currentTimeMillis();
			
			List<RecommendedItem> recommendations = cachingRecommender.recommend(2, 30);
			
			System.out.println("recommendation for 2 takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
			
			System.out.println(recommendations.size());
			
			
			lStart = System.currentTimeMillis();
			
			recommendations = cachingRecommender.recommend(1, 30);
			
			System.out.println("recommendation for 1 takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
			
			System.out.println(recommendations.size());
			
			for (RecommendedItem item : recommendations) {
			//	System.out.println(item);
			}
		} catch (TasteException te) {
			te.printStackTrace();
		}
	}

	@Ignore("Not Ready to Run")
	@Test
	public void testGenericBooleanPrefSlopeOneRecommender() {
		System.out.println("@Test - testGenericBooleanPrefSlopeOneRecommender");
		
		long lStart = System.currentTimeMillis();
		
		try {				
			Recommender recommender = new SlopeOneRecommender(model);
			
			Recommender cachingRecommender = new CachingRecommender(recommender);
			
			System.out.println("building takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
			
			
			lStart = System.currentTimeMillis();
			
			List<RecommendedItem> recommendations = cachingRecommender.recommend(1, 30);
			
			System.out.println("recommendation for 1 takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
			
			System.out.println(recommendations.size());
			
			for (RecommendedItem item : recommendations) {
			//	System.out.println(item);
			}
		} catch (TasteException te) {
			te.printStackTrace();
		}
	}
}
