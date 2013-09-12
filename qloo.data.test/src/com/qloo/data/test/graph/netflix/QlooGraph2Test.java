package com.qloo.data.test.graph.netflix;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.qloo.data.graph.GeoStatus;
import com.qloo.data.graph.netflix.CategoryGenderFilter;
import com.qloo.data.graph.netflix.ChoiceNode;
import com.qloo.data.graph.netflix.QlooGraph2;
import com.qloo.data.graph.netflix.QlooGraphUtil;
import com.qloo.data.graph.netflix.UserNode;
import com.qloo.data.graph.similarity.MaximumUserSimilarityCategoryFilterT;
import com.qloo.data.graph.similarity.TotalUserSimilarityCategoryFilterT;
import com.qloo.data.graph.similarity.UserSimilarityCategoryFilterT;
import com.qloo.data.util.Basic;
import com.qloo.data.util.CategoryUtil;
import com.qloo.data.util.Cosine;
import com.qloo.data.util.SimpleDAO;


public class QlooGraph2Test {
	static QlooGraph2 qg2;
	
	static CategoryGenderFilter cgf = new CategoryGenderFilter();
	static GeoStatus gs = new GeoStatus();

	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		qg2 = QlooGraph2.getInstance("/Users/qloo/work/test2.out");
		
		qg2.setSimpleDAO(new SimpleDAO() {
			@Override
			public Set<UUID> getChoiceUUIDLikeSet(final UUID uid) {
				return new HashSet<UUID>();
			}
			
			@Override
			public Map<UUID, Double> getChoiceUUIDLikeMap(final UUID uid) {
				return new HashMap<UUID, Double>();
			}
			
			@Override
			public Set<UUID> getChoiceUUIDDislikeSet(final UUID uid) {
				return new HashSet<UUID>();
			}
			
			@Override
			public void close() {}
		});
    }
	
	//@Ignore("Not Ready to Run")
	@Test
	public void calculateUserSimilarityAndChoiceNodeRecommendationMap_basic() {
		System.out.println("@Test - calculateUserSimilarityAndChoiceNodeRecommendationMap_basic");
		
		LinkedHashMap<UserNode, Double>[] similarityLHMArray = new LinkedHashMap[CategoryUtil.CATEGORY_COUNT + 1];
		HashMap<ChoiceNode, Double> recMap = new HashMap<ChoiceNode, Double>();

		UserSimilarityCategoryFilterT<UserNode, ChoiceNode> userSimilarity = new TotalUserSimilarityCategoryFilterT<UserNode, ChoiceNode>(new Basic<ChoiceNode>(), cgf);
		
		//UUID uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2");
		UUID uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D0");
		
		long lStart = System.currentTimeMillis();
		
		qg2.calculateUserSimilarityAndChoiceNodeRecommendationMap(similarityLHMArray, recMap, userSimilarity, uid, (short)10, 0.25, (short)0, gs);
				
		System.out.println("calculateUserSimilarityAndChoiceNodeRecommendationMap_basic takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
		
		for (Map.Entry<UserNode, Double> entry : similarityLHMArray[similarityLHMArray.length - 1].entrySet()) {
			System.out.println("score: " + entry.getValue() + "\tuid: " + entry.getKey().uid);
		}
		
		System.out.println("recMap size: " + recMap.size());
		
		LinkedHashMap<ChoiceNode, Double>[] recLHMArray = new LinkedHashMap[8];
		
		QlooGraphUtil.finalizeRecommendationMap(recLHMArray, recMap, (short)25, 0.25, gs);
		
		for (int i = 0; i < recLHMArray.length; i++) {		
			for (Map.Entry<ChoiceNode, Double> entry : recLHMArray[i].entrySet()) {
				System.out.print("score: " + entry.getValue() + "\tcid: " + entry.getKey().cid + "\tcategory: " + entry.getKey().giid + "\ttopics: ");
				
				for (short tid : entry.getKey().tiidSet) {
					System.out.print("\t" + tid);
				}
				
				System.out.println();
			}
		}
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void calculateUserSimilarityAndChoiceNodeRecommendationMap_cosine() {
		System.out.println("@Test - calculateUserSimilarityAndChoiceNodeRecommendationMap_cosine");
		
		UserSimilarityCategoryFilterT<UserNode, ChoiceNode> userSimilarity = new MaximumUserSimilarityCategoryFilterT<UserNode, ChoiceNode>(new Cosine<ChoiceNode>(), cgf);
		
		LinkedHashMap<UserNode, Double>[] similarityLHMArray = new LinkedHashMap[CategoryUtil.CATEGORY_COUNT + 1];
		
		Map<ChoiceNode, Double> recMap = new HashMap<ChoiceNode, Double>();
		
		long lStart = System.currentTimeMillis();
		
		qg2.calculateUserSimilarityAndChoiceNodeRecommendationMap(similarityLHMArray, recMap, userSimilarity, 
				UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2"), (short)10, 0.0, (short)0, gs);
				
		System.out.println("calculateUserSimilarityAndChoiceNodeRecommendationMap_cosine takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
		
		for (Map.Entry<UserNode, Double> entry : similarityLHMArray[similarityLHMArray.length - 1].entrySet()) {
			System.out.println("score: " + entry.getValue() + "\tuid: " + entry.getKey().uid);
		}
		
		/*
		final LinkedHashMap<ChoiceNode, Double> recLHM = new LinkedHashMap<ChoiceNode, Double>(250);
		
		qg2.finalizeRecommendationMap(recLHM, recMap, (short)250, 0.0);
		
		for (Map.Entry<ChoiceNode, Double> entry : recLHM.entrySet()) {
			System.out.println("score: " + entry.getValue() + "\tcid: " + entry.getKey().cid + "\tcategory: " + entry.getKey().gid);
		}*/
		
		LinkedHashMap<ChoiceNode, Double>[] recLHMArray = new LinkedHashMap[CategoryUtil.CATEGORY_COUNT];
		
		QlooGraphUtil.finalizeRecommendationMap(recLHMArray, recMap, (short)250, 0.001, gs);
		
		for (int i = 0; i < recLHMArray.length; i++) {	
			for (Map.Entry<ChoiceNode, Double> entry : recLHMArray[i].entrySet()) {
				System.out.println("score: " + entry.getValue() + "\tcid: " + entry.getKey().cid + "\tcategory: " + entry.getKey().giid + "\ttopic: ");
				
				for (short tid : entry.getKey().tiidSet) {
					System.out.print("\t" + tid);
				}
				
				System.out.println();
			}
		}
	}
}
