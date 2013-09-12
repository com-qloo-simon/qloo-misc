package com.qloo.data.test.graph.netflix.astyanax;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.netflix.astyanax.Keyspace;
import com.qloo.data.cassandra.ChoiceDAO;
import com.qloo.data.cassandra.KSFactory;
import com.qloo.data.cassandra.ProfileDAO;
import com.qloo.data.graph.GeoStatus;
import com.qloo.data.graph.netflix.CategoryGenderFilter;
import com.qloo.data.graph.netflix.ChoiceNode;
import com.qloo.data.graph.netflix.GeoChoiceNode;
import com.qloo.data.graph.netflix.QlooGraph4;
import com.qloo.data.graph.netflix.QlooGraphUtil;
import com.qloo.data.graph.netflix.UserNode;
import com.qloo.data.graph.similarity.MaximumUserSimilarityCategoryFilterT;
import com.qloo.data.graph.similarity.TotalUserSimilarityCategoryFilterT;
import com.qloo.data.graph.similarity.TotalUserSimilarityCategoryListT;
import com.qloo.data.graph.similarity.UserSimilarityCategoryFilterT;
import com.qloo.data.graph.similarity.UserSimilarityCategoryListT;
import com.qloo.data.util.Basic;
import com.qloo.data.util.CategoryUtil;
import com.qloo.data.util.DateUtil;
import com.qloo.data.util.RecommendationConstants;


public class QlooGraph4Test implements RecommendationConstants {
	static QlooGraph4 qg4;
	
	static ProfileDAO pd = new ProfileDAO();
	static ChoiceDAO cd = new ChoiceDAO();
	
	static UUID uid;
	static UserNode un;
	
	//static short neighborSize = 30;
	//static double similarityMin_basic = 1.0;
	//static double similarityMin = 0.0;
	static short recSize = 30;
	//static double scoreMin = 0.001;
	
	static CategoryGenderFilter cgf = new CategoryGenderFilter();
	static List<Short> categoryList = new ArrayList<Short>();
	static short dataOpt = 0;
	static GeoStatus gs = new GeoStatus();

	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		qg4 = QlooGraph4.getInstance("/Users/qloo/work/test4.out");
		
		Keyspace ks = KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3");

	    pd.init(ks, "profile");
	    cd.init(ks, "choice_used");
		
		uid = UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce0");
		
		un = qg4.getUserNode(uid);
		
		//categoryList.add((short)0);  // Music
		//categoryList.add((short)1);  // Film
		//categoryList.add((short)2);
		categoryList.add((short)3);		// Dining
		categoryList.add((short)4);		// Nightlife
		//categoryList.add((short)5);   // Fashion
		//categoryList.add((short)6);
		//categoryList.add((short)7);
		
		gs.cityId = 2;
		gs.lon = -73.993610;
		gs.lat = 40.720915;
    }
    
	//@Ignore("Not Ready to Run")
	@Test
	public void printUserInfo() {
		System.out.println("@Test - printUserInfo");
		
		System.out.println("Graph: gender: " + un.gender + "\tage: " + un.age);
		
		HashMap<String, Object> userMap = pd.readRow(uid, new String[] {"name", "gender", "dob"},
	    		new int[] {pd.COLUMN_TYPE_STRING, pd.COLUMN_TYPE_BOOLEAN, pd.COLUMN_TYPE_TIMESTAMP});
		
		System.out.println("Cassandra: name: " + userMap.get("name") + "\tgender: " + (Boolean)userMap.get("gender") + "\tage: " + DateUtil.getAge(((Date)userMap.get("dob")).getTime()));
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void printChoiceNodeLikeTotalMap() {
		System.out.println("@Test - printChoiceNodeLikeTotalMap");
		
		Map<ChoiceNode, Double> cnMap = qg4.getChoiceNodeLikeMap(un);
		
		System.out.println("total like map size: " + cnMap.size());
		
		Map<ChoiceNode, Double>[] cnMapArray = QlooGraphUtil.splitChoiceNodeMapByCategory(cnMap);
		
		for (int i = 0; i < cnMapArray.length; i++) {
			for (Map.Entry<ChoiceNode, Double> entry: cnMapArray[i].entrySet()) {
				HashMap<String, Object> choiceMap = cd.readRow(entry.getKey().cid, new String[] {"name"}, new int[] {cd.COLUMN_TYPE_STRING});
				
				System.out.println("category: " + CategoryUtil.topCategoryID2NameMap.get(entry.getKey().giid) + "\tname: " + choiceMap.get("name") + "\tscore: " + entry.getValue());
			}
		}
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void printChoiceNodeLikeMap() {
		System.out.println("@Test - printChoiceNodeLikeMap");
		
		Map<ChoiceNode, Double> cnMap = qg4.getChoiceNodeLikeMap(un, false);
		
		System.out.println("like map size: " + cnMap.size());
		
		Map<ChoiceNode, Double>[] cnMapArray = QlooGraphUtil.splitChoiceNodeMapByCategory(cnMap);
		
		for (int i = 0; i < cnMapArray.length; i++) {
			for (Map.Entry<ChoiceNode, Double> entry: cnMapArray[i].entrySet()) {
				HashMap<String, Object> choiceMap = cd.readRow(entry.getKey().cid, new String[] {"name"}, new int[] {cd.COLUMN_TYPE_STRING});
				
				System.out.println("category: " + CategoryUtil.topCategoryID2NameMap.get(entry.getKey().giid) + "\tname: " + choiceMap.get("name") + "\tscore: " + entry.getValue());
			}
		}
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void printChoiceNodeLikeEtherMap() {
		System.out.println("@Test - printChoiceNodeLikeEtherMap");
		
		Map<ChoiceNode, Double> cnMap = qg4.getChoiceNodeLikeMap(un, true);
		
		System.out.println("like ether map size: " + cnMap.size());
		
		Map<ChoiceNode, Double>[] cnMapArray = QlooGraphUtil.splitChoiceNodeMapByCategory(cnMap);
		
		for (int i = 0; i < cnMapArray.length; i++) {
			for (Map.Entry<ChoiceNode, Double> entry: cnMapArray[i].entrySet()) {
				HashMap<String, Object> choiceMap = cd.readRow(entry.getKey().cid, new String[] {"name"}, new int[] {cd.COLUMN_TYPE_STRING});
				
				System.out.println("category: " + CategoryUtil.topCategoryID2NameMap.get(entry.getKey().giid) + "\tname: " + choiceMap.get("name") + "\tscore: " + entry.getValue());
			}
		}
	}

	//@Ignore("Not Ready to Run")
	@Test
	public void printNeighborSetSize() {
		System.out.println("@Test - printNeighborSetSize");
		
		Set<UserNode> neighborSet = qg4.getUserNodeNeighborSet(un, qg4.getChoiceNodeLikeMap(un, false));
		
		System.out.println("neighbor set size: " + neighborSet.size());
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void calculateUserSimilarityAndChoiceNodeRecommendationMap_total_basic() {
		System.out.println("@Test - calculateUserSimilarityAndChoiceNodeRecommendationMap_total_basic");
		
		LinkedHashMap<UserNode, Double>[] similarityLHMArray = new LinkedHashMap[CategoryUtil.CATEGORY_COUNT + 1];
		HashMap<ChoiceNode, Double> recMap = new HashMap<ChoiceNode, Double>();

		UserSimilarityCategoryFilterT<UserNode, ChoiceNode> userSimilarity = new TotalUserSimilarityCategoryFilterT<UserNode, ChoiceNode>(new Basic<ChoiceNode>(), cgf);
		
		long lStart = System.currentTimeMillis();
		
		qg4.calculateUserSimilarityAndChoiceNodeRecommendationMap(similarityLHMArray, recMap, userSimilarity, uid, Neighbor_Size, Similarity_Min_Basic, dataOpt, gs);
				
		System.out.println("calculateUserSimilarityAndChoiceNodeRecommendationMap_basic takes " + (System.currentTimeMillis() - lStart) + " mill seconds");

		LinkedHashMap<ChoiceNode, Double>[] recLHMArray = new LinkedHashMap[8];
		
		QlooGraphUtil.finalizeRecommendationMap(recLHMArray, recMap, recSize, Score_Min_Basic, gs);
		
		printResult(similarityLHMArray, recLHMArray);
	}
	
	//@Ignore("Not Ready to Run")
	@Test
	public void calculateUserSimilarityAndChoiceNodeRecommendationMap_total_category_basic() {
		System.out.println("@Test - calculateUserSimilarityAndChoiceNodeRecommendationMap_total_category_basic");
		
		LinkedHashMap<UserNode, Double>[] similarityLHMArray = new LinkedHashMap[CategoryUtil.CATEGORY_COUNT + 1];
		HashMap<ChoiceNode, Double> recMap = new HashMap<ChoiceNode, Double>();

		UserSimilarityCategoryListT<UserNode, ChoiceNode> userSimilarity = new TotalUserSimilarityCategoryListT<UserNode, ChoiceNode>(new Basic<ChoiceNode>(), cgf, categoryList);
		
		long lStart = System.currentTimeMillis();
		
		qg4.calculateUserSimilarityAndChoiceNodeRecommendationMap(similarityLHMArray, recMap, userSimilarity, uid, Neighbor_Size, Similarity_Min_Basic, dataOpt, gs);
				
		System.out.println("calculateUserSimilarityAndChoiceNodeRecommendationMap_basic takes " + (System.currentTimeMillis() - lStart) + " mill seconds");

		LinkedHashMap<ChoiceNode, Double>[] recLHMArray = new LinkedHashMap[8];
		
		QlooGraphUtil.finalizeRecommendationMap(recLHMArray, recMap, recSize, Score_Min_Basic, gs);
		
		printResult(similarityLHMArray, recLHMArray);
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void calculateUserSimilarityAndChoiceNodeRecommendationMap_max_basic() {
		System.out.println("@Test - calculateUserSimilarityAndChoiceNodeRecommendationMap_max_basic");
		
		LinkedHashMap<UserNode, Double>[] similarityLHMArray = new LinkedHashMap[CategoryUtil.CATEGORY_COUNT + 1];
		HashMap<ChoiceNode, Double> recMap = new HashMap<ChoiceNode, Double>();

		UserSimilarityCategoryFilterT<UserNode, ChoiceNode> userSimilarity = new MaximumUserSimilarityCategoryFilterT<UserNode, ChoiceNode>(new Basic<ChoiceNode>(), cgf);
		
		long lStart = System.currentTimeMillis();
		
		qg4.calculateUserSimilarityAndChoiceNodeRecommendationMap(similarityLHMArray, recMap, userSimilarity, uid, Neighbor_Size, Similarity_Min_Basic, dataOpt, gs);
				
		System.out.println("calculateUserSimilarityAndChoiceNodeRecommendationMap_basic takes " + (System.currentTimeMillis() - lStart) + " mill seconds");

		LinkedHashMap<ChoiceNode, Double>[] recLHMArray = new LinkedHashMap[8];
		
		QlooGraphUtil.finalizeRecommendationMap(recLHMArray, recMap, recSize, Score_Min_Basic, gs);
		
		printResult(similarityLHMArray, recLHMArray);
	}
	
	
	public void printResult(final LinkedHashMap<UserNode, Double>[] similarityLHMArray, final LinkedHashMap<ChoiceNode, Double>[] recLHMArray) {
		for (Map.Entry<UserNode, Double> entry : similarityLHMArray[similarityLHMArray.length - 1].entrySet()) {
			HashMap<String, Object> userMap = pd.readRow(entry.getKey().uid, new String[] {"name"},
		    		new int[] {pd.COLUMN_TYPE_STRING});
			
			System.out.println("score: " + entry.getValue() + "\tname: " + userMap.get("name")  + "\tgender: " + entry.getKey().gender + "\tage: " + entry.getKey().age);
		}
	
		for (int i = 0; i < recLHMArray.length; i++) {
			System.out.println(CategoryUtil.topCategoryID2NameMap.get((short)i));
			
			for (Map.Entry<ChoiceNode, Double> entry : recLHMArray[i].entrySet()) {
				HashMap<String, Object> choiceMap = cd.readRow(entry.getKey().cid, new String[] {"name"}, new int[] {cd.COLUMN_TYPE_STRING});
				
				GeoChoiceNode gcn = (GeoChoiceNode)entry.getKey();
				if (gcn.cityId == 0)
					System.out.println("score: " + entry.getValue() + "\tname: " + choiceMap.get("name"));
				else
					System.out.println("distance: " + (1.0 / entry.getValue() - 1.0) + "\tname: " + choiceMap.get("name") + "\tcityId: " + gcn.cityId + "\tlon: " + gcn.lon + "\tlat: " + gcn.lat);
				
				/*
						"\ttopic: ");
				
				for (short tid : entry.getKey().tiidSet) {
					System.out.print("\t" + tid);
				}
				
				System.out.println();*/
			}
		}
	}
}
