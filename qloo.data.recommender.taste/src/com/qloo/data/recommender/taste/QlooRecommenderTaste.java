package com.qloo.data.recommender.taste;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.AveragingPreferenceInferrer;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qloo.data.graph.netflix.QlooGraph2;

/*
 * integeration of Apache Mahout Taste with Qloo Graph
 */

public class QlooRecommenderTaste {
	private static Logger logger = LoggerFactory.getLogger(QlooRecommenderTaste.class);
	
	private static QlooRecommenderTaste qr;
	public static QlooRecommenderTaste getInstance(final String fname) {
		if (qr == null) {
			qr = new QlooRecommenderTaste();
			//qr.qg2 = QlooGraph2.getInstance("/Users/qloo/work/test2.out");
			qr.qg2 = QlooGraph2.getInstance(fname);
		}
		
		return qr;
	}
	
	private QlooGraph2 qg2 = null;
	private DataModel model = null;
	
	private int cntOfNH = 15;
	private float minSimilarity = 0.7f;
	private int cntOfCR = 30;
	
	private Map<UUID, Map<UUID, Double>> userSimilarMap = new ConcurrentHashMap<UUID, Map<UUID, Double>>();
	private Map<UUID, Map<UUID, Double>> choiceRecommenderMap = new ConcurrentHashMap<UUID, Map<UUID, Double>>();
	
	private UserSimilarity userSimilarity = null;
	private UserNeighborhood neighborhood = null;
	private Recommender cachingRecommender = null;
	
	public void init(final String fname, final int cntOfNH, final float minSimilarity, final int cntOfCR) {		
		try {
			model = new GenericDataModel(new FileDataModel(new File(fname)));
		} catch (TasteException te) {
			model = null; te.printStackTrace();
		} catch (IOException ioe) {
			model = null; ioe.printStackTrace();
		} catch (NoSuchElementException nse) {
			model = null; nse.printStackTrace();
		} catch (Throwable th) {
			model = null; th.printStackTrace();
		}
		
		if (model == null) return;
		
		this.cntOfNH = cntOfNH;
		this.minSimilarity = minSimilarity;
		this.cntOfCR = cntOfCR;
		
		long lStart = System.currentTimeMillis();
		
		try {	
			//this.userSimilarity = new EuclideanDistanceSimilarity(model);
			this.userSimilarity = new PearsonCorrelationSimilarity(model);
			
			this.userSimilarity.setPreferenceInferrer(new AveragingPreferenceInferrer(model));
			
			neighborhood = new NearestNUserNeighborhood(this.cntOfNH, this.minSimilarity, this.userSimilarity, model);
			
			// long[] nbs = neighborhood.getUserNeighborhood(1);
			// System.out.println("neighborhood count: " + nbs.length);
			
			Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, userSimilarity);
			
			cachingRecommender = new CachingRecommender(recommender);
			
			logger.info("building takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
		} catch (TasteException te) {
			logger.error(te.getMessage(), te);
		}
	}
	
	
	public Map<UUID, Double> getUserSimilarityMap(final UUID uid) {
		Map<UUID, Double> map = userSimilarMap.get(uid);
		if (map != null) return map;
		
		long lStart = System.currentTimeMillis();
		
		try {
			final int userOrdinal = qg2.getUserOrdinal(uid);
			
			logger.info("userOrdinal: " + userOrdinal);
			
			long[] nhs = neighborhood.getUserNeighborhood(userOrdinal);
			
			logger.info("neighborhood count: " + nhs.length);
			
			map = new HashMap<UUID, Double>(cntOfNH);
			
			for (long nh : nhs) {
				map.put(qg2.getUserUUID((int)nh), userSimilarity.userSimilarity(userOrdinal, nh));
			}
			
			userSimilarMap.put(uid, map);
			
			logger.info("finding " + nhs.length + " similarity takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
		} catch (TasteException te) {
			logger.error(te.getMessage(), te);
		} catch (Throwable th) {
			th.printStackTrace();
		}
		
		return map;
	}
	
	public Map<UUID, Double> getChoiceRecommendationMap(final UUID uid) {
		Map<UUID, Double> map = choiceRecommenderMap.get(uid);
		if (map != null) return map;
		
		long lStart = System.currentTimeMillis();
		
		try {
			final int userOrdinal = qg2.getUserOrdinal(uid);
			
			List<RecommendedItem> recommendations = cachingRecommender.recommend(userOrdinal, cntOfCR);

			logger.info("recommendation takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
			
			map = new HashMap<UUID, Double>(cntOfCR);
			
			for (RecommendedItem item : recommendations) {
				map.put(qg2.getUserUUID((int)item.getItemID()), (double)item.getValue());
			}
			
			choiceRecommenderMap.put(uid, map);
		} catch (TasteException te) {
			logger.error(te.getMessage(), te);
		}
		
		return map;
	}
}
