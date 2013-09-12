package com.qloo.data.test.graph.netflix.astyanax;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.netflix.astyanax.Keyspace;
import com.qloo.data.cassandra.ChoiceDAO;
import com.qloo.data.cassandra.KSFactory;
import com.qloo.data.cassandra.ProfileDAO;
import com.qloo.data.graph.netflix.QlooGraph;
import com.qloo.data.graph.netflix.QlooGraphUtil;
import com.qloo.data.graph.netflix.UserNode;
import com.qloo.data.graph.netflix.ChoiceNode;
import com.qloo.data.util.CategoryUtil;
import com.qloo.data.util.DateUtil;


public class QlooGraphTest {
	static QlooGraph qg;
	
	static ProfileDAO pd = new ProfileDAO();
	static ChoiceDAO cd = new ChoiceDAO();
	
	static UUID uid;
	static UserNode un;

	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		qg = QlooGraph.getInstance("/Users/qloo/work/test.out");
		
		Keyspace ks = KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3");

	    pd.init(ks, "profile");
	    cd.init(ks, "choice_used");
		
		uid = UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce0");
		
		un = qg.getUserNode(uid);
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
	public void printLikeList() {
		System.out.println("@Test - printLikeList");
		
		List<ChoiceNode> cnList = qg.getChoiceNodeLikeList(un);
		
		System.out.println("like list size: " + cnList.size());
		
		List<ChoiceNode>[] cnListArray = QlooGraphUtil.splitChoiceNodeListByCategory(cnList);
		
		for (int i = 0; i < cnListArray.length; i++) {
			for (ChoiceNode cn : cnListArray[i]) {
				HashMap<String, Object> choiceMap = cd.readRow(cn.cid, new String[] {"name"}, new int[] {cd.COLUMN_TYPE_STRING});
				
				System.out.println("category: " + CategoryUtil.topCategoryID2NameMap.get(cn.giid) + "\tcid: " + cn.cid + "\tname: " + choiceMap.get("name"));
			}
		}
	}

	//@Ignore("Not Ready to Run")
	@Test
	public void printSortedSimilarUsers_basic() {
		System.out.println("@Test - printSortedSimilarUsers_basic");
		
		long lStart = System.currentTimeMillis();
		
		LinkedHashMap<UserNode, Integer> lhm = qg.getUserNodeSimilarMap(un, (short)30);
		
		System.out.println("printSortedSimilarUsers takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
		
		for (Map.Entry<UserNode, Integer> entry : lhm.entrySet()) {
			HashMap<String, Object> userMap = pd.readRow(entry.getKey().uid, new String[] {"name", "gender", "dob"},
		    		new int[] {pd.COLUMN_TYPE_STRING, pd.COLUMN_TYPE_BOOLEAN, pd.COLUMN_TYPE_TIMESTAMP});
			
			System.out.println("score: " + entry.getValue() + "\tname: " + userMap.get("name") + "\tgender: " + (Boolean)userMap.get("gender") + "\tage: " + DateUtil.getAge(((Date)userMap.get("dob")).getTime()));
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
			HashMap<String, Object> choiceMap = cd.readRow(entry.getKey().cid, new String[] {"name"}, new int[] {cd.COLUMN_TYPE_STRING});
			
			System.out.println("score: " + entry.getValue() +  "\tcategory: " + CategoryUtil.topCategoryID2NameMap.get(entry.getKey().giid) + "\tname: " + choiceMap.get("name"));

			count++; if (count >= 100) break;
		}
	}
}
