package com.qloo.data.test.graph.netflix.astyanax;

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

import com.netflix.astyanax.Keyspace;
import com.qloo.data.cassandra.ChoiceDAO;
import com.qloo.data.cassandra.KSFactory;
import com.qloo.data.cassandra.ProfileDAO;
import com.qloo.data.cassandra.TopicDAO;
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
	
	static ProfileDAO pd = new ProfileDAO();
	static ChoiceDAO cd = new ChoiceDAO();
	//static TopicDAO td = new TopicDAO();
	static Map<Integer, TopicDAO.TopicInfo> topicInfoMap = null;
	
	static UUID uid;
	static UserNode un;
	
	static CategoryGenderFilter cgf = new CategoryGenderFilter();
	static List<Short> categoryList = new ArrayList<Short>();
	static short dataOpt = 0;
	static GeoStatus geo = new GeoStatus();
	

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
		
		Keyspace ks = KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3");

	    pd.init(ks, "profile");
	    cd.init(ks, "choice_used");
	    
	    TopicDAO td = new TopicDAO();
	    td.init(ks, "topic");
	    
	    Map<UUID, TopicDAO.TopicInfo> tMap = td.readAllTopicInfoMap();
	    
	    topicInfoMap = new HashMap<Integer, TopicDAO.TopicInfo>();
	    for (Map.Entry<UUID, TopicDAO.TopicInfo> entry : tMap.entrySet()) {
	    	topicInfoMap.put(entry.getValue().iid, entry.getValue());
	    }
	   
		
		categoryList.add((short)0);
		categoryList.add((short)1);
		categoryList.add((short)2);
		//partialList.add((short)3);
		//partialList.add((short)4);
		//partialList.add((short)5);
		//partialList.add((short)6);
		//partialList.add((short)7);
		
		uid = UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce0");
		
		un = qg3.getUserNode(uid);
    }
	
	//@Ignore("Not Ready to Run")
	@Test
	public void calculateUserSimilarityAndChoiceNodeRecommendationMap_basic() {
		System.out.println("@Test - calculateUserSimilarityAndChoiceNodeRecommendationMap_basic");
		
		calculateUserSimilarityAndChoiceNodeRecommendationMap(new TotalUserSimilarityCategoryFilterT<UserNode, ChoiceNode>(new Basic<ChoiceNode>(), cgf), Similarity_Min_Basic, Score_Min_Basic);
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void calculateUserSimilarityAndChoiceNodeRecommendationMap_basic_category() {
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
				uid, Neighbor_Size, similarityMin, dataOpt, geo);
				
		System.out.println("calculateUserSimilarityAndChoiceNodeRecommendationMap takes " + (System.currentTimeMillis() - lStart) + " mill seconds");

		QlooGraphUtil.finalizeRecommendationMap(recLHMArray, recMap, Rec_Size_Category, scoreMin, geo);
		
		printResult(similarityLHMArray, recLHMArray);
	}
	
	public void printResult(final LinkedHashMap<UserNode, Double>[] similarityLHMArray, final LinkedHashMap<ChoiceNode, Double>[] recLHMArray) {
		for (Map.Entry<UserNode, Double> entry : similarityLHMArray[similarityLHMArray.length - 1].entrySet()) {
		    HashMap<String, Object> userMap = pd.readRow(entry.getKey().uid, new String[] {"uname", "name"},
		    		new int[] {pd.COLUMN_TYPE_STRING, pd.COLUMN_TYPE_STRING});
			
		    String gender = null;
		    if (entry.getKey().gender) gender = "female";
		    else gender = "male";
		    
			System.out.println("score: " + entry.getValue() + "\tuname: " + userMap.get("uname") + "\tname: " + userMap.get("name") + "\tgender: " + gender + "\tage: " + entry.getKey().age);
		}
		
		for (int i = 0; i < recLHMArray.length; i++) {
			int count = 0;
			
			for (Map.Entry<ChoiceNode, Double> entry : recLHMArray[i].entrySet()) {
				HashMap<String, Object> choiceMap = cd.readRow(entry.getKey().cid, new String[] {"name"}, new int[] {cd.COLUMN_TYPE_STRING});
				
				System.out.print("score: " + entry.getValue() + "\tname: " + choiceMap.get("name") + "\tcategory: " + CategoryUtil.topCategoryID2NameMap.get(entry.getKey().giid) + "\ttopic: ");
				
				for (short tiid : entry.getKey().tiidSet) {
					System.out.print("\t" + (topicInfoMap.get((int)tiid)).name);
				}
				
				System.out.println();
				
				count++; if (count > 50) break;
			}
		}
	}
}
