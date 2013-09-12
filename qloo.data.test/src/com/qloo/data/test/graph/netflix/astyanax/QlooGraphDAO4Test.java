package com.qloo.data.test.graph.netflix.astyanax;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.Date;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.netflix.astyanax.Keyspace;
import com.qloo.data.cassandra.ChoiceDAO;
import com.qloo.data.cassandra.KSFactory;
import com.qloo.data.cassandra.ProfileDAO;
import com.qloo.data.cassandra.TopicDAO;
import com.qloo.data.graph.netflix.ChoiceNode;
import com.qloo.data.graph.netflix.QlooGraphDAO4;
import com.qloo.data.graph.netflix.QlooGraphUtil;
import com.qloo.data.graph.netflix.UserNode;
import com.qloo.data.util.DateUtil;
import com.qloo.data.util.CategoryUtil;


public class QlooGraphDAO4Test {
	static QlooGraphDAO4 qgd4;
	
	static ProfileDAO pd = new ProfileDAO();
	static ChoiceDAO cd = new ChoiceDAO();
	static TopicDAO td = new TopicDAO();
	
	static UUID uid;
	static UserNode un;

	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		qgd4 = new QlooGraphDAO4();
		
		try {
			qgd4.input("/Users/qloo/work/test4.out");
	    } catch (IOException ioe) {
	    	ioe.printStackTrace();
	    	return;
	    }
		
		Keyspace ks = KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3");

	    pd.init(ks, "profile");
	    cd.init(ks, "choice_used");
	    td.init(ks, "topic");
	    
	    //uid = UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce0");
	    //uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2");
	    //uid = UUID.fromString("d34aa494-68d4-49d0-8b0f-652b0a8572aa");
	    uid = UUID.fromString("e32b8d38-7674-4bde-959a-003f05c135bc");
	    
	    un = qgd4.getUserNode(uid);
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
	public void printLikeTotalList() {
		System.out.println("@Test - printLikeTotalList");
		
		List<ChoiceNode> cnList = qgd4.getChoiceNodeLikeList(un);
		
		System.out.println("total like list size: " + cnList.size());
		
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
	public void printLikeList() {
		System.out.println("@Test - printLikeList");
		
		List<ChoiceNode> cnList = qgd4.getChoiceNodeLikeList(un, false);
		
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
	public void printLikeEtherList() {
		System.out.println("@Test - printLikeEtherList");
		
		List<ChoiceNode> cnList = qgd4.getChoiceNodeLikeList(un, true);
		
		System.out.println("ether like list size: " + cnList.size());
		
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
	public void printDislikeList() {
		System.out.println("@Test - printDislikeList");
		
		List<ChoiceNode> cnList = qgd4.getChoiceNodeDislikeList(un);
		
		System.out.println("dislike list size: " + cnList.size());
		
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
	public void printSimilarUsers() {
		System.out.println("@Test - printSimilarUsers");
		
		HashSet<UserNode> neighborSet = new HashSet<UserNode>();
		
		long lStart = System.currentTimeMillis();
		
	    List<ChoiceNode> likeList = qgd4.getChoiceNodeLikeList(un, false);

	    for (ChoiceNode cn : likeList) {
	    	List<UserNode> likedList = qgd4.getUserNodeLikedList(cn, false);
	    	neighborSet.addAll(likedList);
	    }
	    
	    List<ChoiceNode> dislikeList = qgd4.getChoiceNodeDislikeList(un);

	    for (ChoiceNode cn : dislikeList) {
	    	List<UserNode> dislikedList = qgd4.getUserNodeDislikedList(cn);
	    	neighborSet.addAll(dislikedList);
	    }
	   
	    System.out.println("like count: " + likeList.size() + "\tdislike count: " + dislikeList.size() + " unique similar user count: " + neighborSet.size() + " takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void printPopularLHM() {
		System.out.println("@Test - printPopularLHM");
		
	    Map<UUID, TopicDAO.TopicInfo> tMap = td.readAllTopicInfoMap();
	    
	    Map<Integer, TopicDAO.TopicInfo> topicInfoMap = new HashMap<Integer, TopicDAO.TopicInfo>();
	    for (Map.Entry<UUID, TopicDAO.TopicInfo> entry : tMap.entrySet()) {
	    	topicInfoMap.put(entry.getValue().iid, entry.getValue());
	    }
	    
	    System.out.println("topicInfoMap size: " + topicInfoMap.size());
		
		for (int i = 0; i < qgd4.getPopularLHMArray().length; i++) {
			for (Map.Entry<Short, LinkedHashMap<Integer, Double>> entry : qgd4.getPopularLHMArray()[i].entrySet()) {
				LinkedHashMap<Integer, Double> lhm = entry.getValue();
				
				//System.out.print("giid: " + i + "\ttiid: " + entry.getKey() + "\tsize: " + lhm.size());
				System.out.print(CategoryUtil.topCategoryID2NameMap.get((short)i) + "\t" + topicInfoMap.get((int)entry.getKey()).name + "\tsize: " + lhm.size());
				
				for (Map.Entry<Integer, Double> entry2 : lhm.entrySet()) {
					int choiceOrdinal = entry2.getKey();
					ChoiceNode cn = qgd4.getChoiceNode(choiceOrdinal);
					HashMap<String, Object> choiceMap = cd.readRow(cn.cid, new String[] {"name"}, new int[] {cd.COLUMN_TYPE_STRING});
					
					System.out.print("\t" + choiceMap.get("name") + " " + entry2.getValue());
				}
				
				System.out.println();
			}
		}
	}
	
	
	//@Ignore("Not Ready to Run")
	@Test
	public void printFollowList() {
		System.out.println("@Test - printFollowList");
		
	    List<UserNode> followList = qgd4.getUserNodeFollowList(UserNode.newUserNode(uid));
	    
	    System.out.println("follow count: " + followList.size());

	    for (UserNode un : followList) {
		    HashMap<String, Object> userMap = pd.readRow(un.uid, new String[] {"name"},
		    		new int[] {pd.COLUMN_TYPE_STRING});
		    
	    	System.out.println("name: " + userMap.get("name") + "\tgender: " + un.gender + "\tage: " + un.age);
	    }
	}
	
	//@Ignore("Not Ready to Run")
	@Test
	public void printFollowedList() {
		System.out.println("@Test - printFollowedList");
		
	    List<UserNode> followedList = qgd4.getUserNodeFollowedList(UserNode.newUserNode(uid));
	    
	    System.out.println("followed count: " + followedList.size());

	    for (UserNode un : followedList) {
		    HashMap<String, Object> userMap = pd.readRow(un.uid, new String[] {"name"},
		    		new int[] {pd.COLUMN_TYPE_STRING});
		    
	    	System.out.println("name: " + userMap.get("name") + "\tgender: " + un.gender + "\tage: " + un.age);
	    }
	}
}
