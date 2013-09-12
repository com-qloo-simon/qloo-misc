package com.qloo.data.test.recommender.astyanax;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.netflix.astyanax.Keyspace;
import com.qloo.data.recommender.QlooRecommender;
import com.qloo.data.util.CategoryUtil;
import com.qloo.data.cassandra.ChoiceDAO;
import com.qloo.data.cassandra.KSFactory;
import com.qloo.data.cassandra.ProfileDAO;
import com.qloo.data.graph.GeoStatus;
import com.qloo.data.graph.netflix.ChoiceNode;
import com.qloo.data.graph.netflix.GeoChoiceNode;
import com.qloo.data.graph.netflix.UserNode;


public class QlooRecommenderTest {
	static QlooRecommender qr;
	
	static ProfileDAO pd = new ProfileDAO();
	static ChoiceDAO cd = new ChoiceDAO();
	
	static UUID uid;
	static UserNode un;
	
	static short choiceOpt = 0;
	
	static List<Short> categoryList = new ArrayList<Short>();
	static short categoryOpt = 0;
	static short dataOpt = 1;
	static short pageOpt = 0;
	
	static GeoStatus gs = new GeoStatus();
	
	
	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		qr = QlooRecommender.getInstance("/Users/qloo/work/test4.out");
		
		Keyspace ks = KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3");

	    pd.init(ks, "profile");
	    cd.init(ks, "choice_used");
		
		uid = UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce0");
		// uid = UUID.fromString("d34aa494-68d4-49d0-8b0f-652b0a8572aa");
		// uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2");
		// uid = UUID.fromString("b55216ae-3fc8-4bb9-80db-29d2c8cc151a");
		// uid = UUID.fromString("e0ccf3be-0e92-4860-82ad-12eab3bd6113");
		// uid = UUID.fromString("afe381ce-7087-4d1a-91ab-98e63e1e84f4");
		// uid = UUID.fromString("E32B8D38-7674-4BDE-959A-003F05C135BC");
	    // uid = UUID.fromString("e32b8d38-7674-4bde-959a-003f05c135bc");
		
		categoryList.add((short)0);  	// Music
		categoryList.add((short)1);  	// Film
		categoryList.add((short)2);
		categoryList.add((short)3);		// Dining
		categoryList.add((short)4);		// Nightlife
		categoryList.add((short)5);   	// Fashion
		categoryList.add((short)6);
		categoryList.add((short)7);
		
		categoryOpt = 3;
		
		gs.cityId = 2;
		gs.lon = -73.993610;
		gs.lat = 40.720915;
	}
	
    @AfterClass
    public static void oneTimeTearDown() {
    	System.out.println("@AfterClass - oneTimeTearDown");
    	
    	qr.close();
    }
	
	@Ignore("Not Ready to Run")
	@Test
	public void printUserSimilarMap_basic() {
		System.out.println("@Test - printUserSimilarMap_basic");
		
		Map<UserNode, Double> map = qr.getUserSimilarityMap(uid, (short)0, choiceOpt, dataOpt, pageOpt);
		//Map<UserNode, Double> map = qr.getUserSimilarityMap(uid, (short)0, (short)0, categoryList, (short)1, (short)0);
		
		System.out.println(map.size());
		
		for (Map.Entry<UserNode, Double> entry : map.entrySet()) {
			HashMap<String, Object> userMap = pd.readRow(entry.getKey().uid, new String[] {"name", "gender", "dob"},
		    		new int[] {pd.COLUMN_TYPE_STRING, pd.COLUMN_TYPE_BOOLEAN, pd.COLUMN_TYPE_TIMESTAMP});
			
			System.out.println("score: " + entry.getValue() + "\tname: " + userMap.get("name") + "\tgender: " + entry.getKey().gender + "\tage: " + entry.getKey().age);
		}
	}
	
	//@Ignore("Not Ready to Run")
	@Test
	public void printChoiceRecommenderMap_basic() {
		System.out.println("@Test - printChoiceRecommenderMap_basic");
		
		LinkedHashMap<ChoiceNode, Double> recLHM = qr.getChoiceRecommendationMap(uid, (short)0, choiceOpt, categoryOpt, dataOpt, pageOpt, gs);
		//LinkedHashMap<ChoiceNode, Double> recLHM = qr.getChoiceRecommendationMap(uid, (short)0, (short)0, categoryList, categoryOpt, (short)1, (short)1, gs);
		
		System.out.println("category: " + CategoryUtil.topCategoryID2NameMap.get(categoryOpt));
		
		int count = 0;
		for (Map.Entry<ChoiceNode, Double> entry : recLHM.entrySet()) {
			HashMap<String, Object> choiceMap = cd.readRow(entry.getKey().cid, new String[] {"name"}, new int[] {cd.COLUMN_TYPE_STRING});

			GeoChoiceNode gcn = (GeoChoiceNode)entry.getKey();
			if (gcn.cityId == 0)
				System.out.println("score: " + entry.getValue() + "\tname: " + choiceMap.get("name"));
			else
				System.out.println("distance: " + (1.0 / entry.getValue() - 1.0) + "\tname: " + choiceMap.get("name") + "\tcityId: " + gcn.cityId + "\tlon: " + gcn.lon + "\tlat: " + gcn.lat);

			/*
			System.out.print("score: " + entry.getValue() + "\tname: " + choiceMap.get("name") +  "\ttopic: ");
			
			for (short tid : entry.getKey().tiidSet) {
				System.out.print("\t" + tid);
			}
			
			System.out.println();
			*/
			
			count++; if (count > 100) break;
		}
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void printUserSimilarMap_cosine() {
		System.out.println("@Test - printUserSimilarMap_cosine");
		
		//Map<UserNode, Double> map = qr.getUserSimilarMap(uid, (short)0, (short)0);
		Map<UserNode, Double> map = qr.getUserSimilarityMap(uid, (short)1, (short)0, categoryList, (short)0, (short)0);
		
		System.out.println(map.size());
		
		for (Map.Entry<UserNode, Double> entry : map.entrySet()) {
			System.out.println("score: " + entry.getValue() + "\tuid: " + entry.getKey().uid + "\tgender: " + entry.getKey().gender + "\tage: " + entry.getKey().age);
		}
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void printChoiceRecommenderMap_cosine() {
		System.out.println("@Test - printChoiceRecommenderMap_cosine");
		
		//LinkedHashMap<ChoiceNode, Double> recLHM = qr.getChoiceRecommendationMap(uid, (short)0, (short)0, (short)0);
		LinkedHashMap<ChoiceNode, Double> recLHM = qr.getChoiceRecommendationMap(uid, (short)1, (short)0, (short)0, (short)0, (short)0, gs);
		
		for (Map.Entry<ChoiceNode, Double> entry : recLHM.entrySet()) {
			System.out.print("score: " + entry.getValue() + "\tcid: " + entry.getKey().cid + "\tcategory: " + entry.getKey().giid + "\ttopic: ");
			
			for (short tid : entry.getKey().tiidSet) {
				System.out.print("\t" + tid);
			}
			
			System.out.println();
		}
	}
}
