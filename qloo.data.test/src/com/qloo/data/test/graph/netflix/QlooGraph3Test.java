package com.qloo.data.test.graph.netflix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.qloo.data.graph.GeoStatus;
import com.qloo.data.graph.netflix.CategoryGenderFilter;
import com.qloo.data.graph.netflix.ChoiceNode;
import com.qloo.data.graph.netflix.QlooGraph3;
import com.qloo.data.graph.netflix.UserNode;
import com.qloo.data.graph.netflix.QlooGraphUtil;
import com.qloo.data.graph.similarity.*;
import com.qloo.data.util.Basic;
import com.qloo.data.util.Cosine;
import com.qloo.data.util.Euclidean;
import com.qloo.data.util.Pearson;
import com.qloo.data.util.SimpleDAO;
import com.qloo.data.util.CategoryUtil;
import com.qloo.data.util.RecommendationConstants;


public class QlooGraph3Test implements RecommendationConstants {
	static QlooGraph3 qg3;
	
	static CategoryGenderFilter cgf = new CategoryGenderFilter();
	static List<Short> categoryList = new ArrayList<Short>();
	static GeoStatus gs = new GeoStatus();
	
	static UUID uid;
	static short dataOpt = 0;

	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		qg3 = QlooGraph3.getInstance("/Users/qloo/work/test3.out");
		
		qg3.setSimpleDAO(new SimpleDAO() {
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
		
		categoryList.add((short)0);
		categoryList.add((short)1);
		categoryList.add((short)2);
		//categoryList.add((short)3);
		//categoryList.add((short)4);
		//categoryList.add((short)5);
		//categoryList.add((short)6);
		//categoryList.add((short)7);
		
		//uid = UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce0");
		uid = UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce1");
    }
	
	//@Ignore("Not Ready to Run")
	@Test
	public void calculateUserSimilarityAndChoiceNodeRecommendationMap_basic() {
		System.out.println("@Test - calculateUserSimilarityAndChoiceNodeRecommendationMap_basic");
		
		calculateUserSimilarityAndChoiceNodeRecommendationMap(new TotalUserSimilarityCategoryListT<UserNode, ChoiceNode>(new Basic<ChoiceNode>(), cgf, categoryList), Similarity_Min_Basic, Score_Min_Basic);
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void calculateUserSimilarityAndChoiceNodeRecommendationMap_cosine() {
		System.out.println("@Test - calculateUserSimilarityAndChoiceNodeRecommendationMap_cosine");
		
		calculateUserSimilarityAndChoiceNodeRecommendationMap(new MaximumUserSimilarityCategoryListT<UserNode, ChoiceNode>(new Cosine<ChoiceNode>(), cgf, categoryList), Similarity_Min, Score_Min_Basic);
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void calculateUserSimilarityAndChoiceNodeRecommendationMap_euclidean() {
		System.out.println("@Test - calculateUserSimilarityAndChoiceNodeRecommendationMap_euclidean");
		
		calculateUserSimilarityAndChoiceNodeRecommendationMap(new MaximumUserSimilarityCategoryListT<UserNode, ChoiceNode>(new Euclidean<ChoiceNode>(), cgf, categoryList), Similarity_Min, Score_Min_Basic);
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void calculateUserSimilarityAndChoiceNodeRecommendationMap_pearson() {
		System.out.println("@Test - calculateUserSimilarityAndChoiceNodeRecommendationMap_pearson");
		
		calculateUserSimilarityAndChoiceNodeRecommendationMap(new MaximumUserSimilarityCategoryListT<UserNode, ChoiceNode>(new Pearson<ChoiceNode>(), cgf, categoryList), Similarity_Min_Pearson, Score_Min_Pearson);
	}
	
	public void calculateUserSimilarityAndChoiceNodeRecommendationMap(final UserSimilarityCategoryFilterT<UserNode, ChoiceNode> userSimilarity, final double similarityMin, final double scoreMin) {
		LinkedHashMap<UserNode, Double>[] similarityLHMArray = new LinkedHashMap[CategoryUtil.CATEGORY_COUNT + 1];
		
		LinkedHashMap<ChoiceNode, Double>[] recLHMArray = new LinkedHashMap[CategoryUtil.CATEGORY_COUNT];
		
		Map<ChoiceNode, Double> recMap = new HashMap<ChoiceNode, Double>();
		
		long lStart = System.currentTimeMillis();
		
		qg3.calculateUserSimilarityAndChoiceNodeRecommendationMap(similarityLHMArray, recMap, userSimilarity, 
				uid, Neighbor_Size, similarityMin, dataOpt, gs);
				
		System.out.println("calculateUserSimilarityAndChoiceNodeRecommendationMap takes " + (System.currentTimeMillis() - lStart) + " mill seconds");

		QlooGraphUtil.finalizeRecommendationMap(recLHMArray, recMap, Rec_Size_Category, scoreMin, gs);
		
		printResult(similarityLHMArray, recLHMArray);
	}
	
	public void printResult(final LinkedHashMap<UserNode, Double>[] similarityLHMArray, final LinkedHashMap<ChoiceNode, Double>[] recLHMArray) {
		for (Map.Entry<UserNode, Double> entry : similarityLHMArray[similarityLHMArray.length - 1].entrySet()) {
			System.out.println("score: " + entry.getValue() + "\tuid: " + entry.getKey().uid + "\tgender: " + entry.getKey().gender + "\tage: " + entry.getKey().age);
		}
	
		for (int i = 0; i < recLHMArray.length; i++) {	
			for (Map.Entry<ChoiceNode, Double> entry : recLHMArray[i].entrySet()) {
				System.out.print("score: " + entry.getValue() + "\tcid: " + entry.getKey().cid + "\tcategory: " + entry.getKey().giid + "\ttopic: ");
				
				for (short tid : entry.getKey().tiidSet) {
					System.out.print("\t" + tid);
				}
				
				System.out.println();
			}
		}
	}
}
