package com.qloo.data.recommender.taste.test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.impl.similarity.UncenteredCosineSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.junit.BeforeClass;
import org.junit.Test;

public class TasteTest2 {
	static DataModel model;
	
	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		long lStart = System.currentTimeMillis();
		
		try {
			// model = new GenericBooleanPrefDataModel(new FileDataModel(new File("/Users/qloo/work/test2.csv")));
			model = new GenericDataModel(new FileDataModel(new File("/Users/qloo/work/test2.csv")));
		} catch (TasteException te) {
			te.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (NoSuchElementException nse) {
			nse.printStackTrace();
		}
		
		System.out.println("setup takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
	}
	
	@Test
	public void testGenericUserBasedRecommender() {
		System.out.println("@Test - testGenericUserBasedRecommender");
		
		long lStart = System.currentTimeMillis();
		
		try {	
			// UserSimilarity userSimilarity = new PearsonCorrelationSimilarity(model);
			// UserSimilarity userSimilarity = new LogLikelihoodSimilarity(model);
			UserSimilarity userSimilarity = new UncenteredCosineSimilarity(model);
			
			userSimilarity.setPreferenceInferrer(new AveragingPreferenceInferrer(model));
			
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(10, 0.7, userSimilarity, model);
			
			long[] nbs = neighborhood.getUserNeighborhood(1);
			System.out.println("neighborhood count: " + nbs.length);
			
			Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, userSimilarity);
			
			Recommender cachingRecommender = new CachingRecommender(recommender);
			
			long lEnd = System.currentTimeMillis();
			System.out.println("building takes " + (lEnd - lStart) + " mill seconds");
			lStart = lEnd;
			
			List<RecommendedItem> recommendations = cachingRecommender.recommend(1, 10);
			
			System.out.println("recommendation takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
			
			System.out.println(recommendations.size());
			
			for (RecommendedItem item : recommendations) {
				System.out.println(item);
			}
		} catch (TasteException te) {
			te.printStackTrace();
		} catch (Throwable th) {
			th.printStackTrace();
		}
	}
}
