package com.qloo.data.test.graph.netflix.astyanax;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.Date;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.netflix.astyanax.Keyspace;
import com.qloo.data.cassandra.ChoiceDAO;
import com.qloo.data.cassandra.KSFactory;
import com.qloo.data.cassandra.ProfileDAO;
import com.qloo.data.graph.netflix.QlooGraphUtil;
import com.qloo.data.graph.netflix.ChoiceNode;
import com.qloo.data.graph.netflix.QlooGraphDAO2;
import com.qloo.data.graph.netflix.UserNode;
import com.qloo.data.util.DateUtil;
import com.qloo.data.util.CategoryUtil;


public class QlooGraphDAO2Test {
	static QlooGraphDAO2 qgd2;
	
	static ProfileDAO pd = new ProfileDAO();
	static ChoiceDAO cd = new ChoiceDAO();
	
	static UUID uid;
	static UserNode un;

	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		qgd2 = new QlooGraphDAO2();
		
		try {
			qgd2.input("/Users/qloo/work/test2.out");
	    } catch (IOException ioe) {
	    	ioe.printStackTrace();
	    	return;
	    }
		
		Keyspace ks = KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3");

	    pd.init(ks, "profile");
	    cd.init(ks, "choice_used");
	    
	    uid = UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce0");
	    //uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2");
	    //uid = UUID.fromString("b6704657-732a-416d-9de4-07a22270e9d8");
	    
	    un = qgd2.getUserNode(uid);
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
	public void printTotalLikeList() {
		System.out.println("@Test - printTotalLikeList");
		
		List<ChoiceNode> cnList = qgd2.getChoiceNodeLikeList(un);
		
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
	public void printLikeList() {
		System.out.println("@Test - printLikeList");
		
		List<ChoiceNode> cnList = qgd2.getChoiceNodeLikeList(un, false);
		
		System.out.println("top like list size: " + cnList.size());
		
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
		
		List<ChoiceNode> cnList = qgd2.getChoiceNodeLikeList(un, true);
		
		System.out.println("top like list size: " + cnList.size());
		
		List<ChoiceNode>[] cnListArray = QlooGraphUtil.splitChoiceNodeListByCategory(cnList);
		
		for (int i = 0; i < cnListArray.length; i++) {
			for (ChoiceNode cn : cnListArray[i]) {
				HashMap<String, Object> choiceMap = cd.readRow(cn.cid, new String[] {"name"}, new int[] {cd.COLUMN_TYPE_STRING});
				
				System.out.println("category: " + CategoryUtil.topCategoryID2NameMap.get(cn.giid) + "\tcid: " + cn.cid + "\tname: " + choiceMap.get("name"));
			}
		}
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void printFollowList() {
		System.out.println("@Test - printFollowList");
		
	    List<UserNode> followList = qgd2.getUserNodeFollowList(UserNode.newUserNode(uid));
	    
	    System.out.println("follow count: " + followList.size());

	    for (UserNode un : followList) {
		    HashMap<String, Object> userMap = pd.readRow(un.uid, new String[] {"name"},
		    		new int[] {pd.COLUMN_TYPE_STRING});
		    
	    	System.out.println("name: " + userMap.get("name") + "\tgender: " + un.gender + "\tage: " + un.age);
	    }
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void printFollowedList() {
		System.out.println("@Test - printFollowedList");
		
	    List<UserNode> followedList = qgd2.getUserNodeFollowedList(UserNode.newUserNode(uid));
	    
	    System.out.println("followed count: " + followedList.size());

	    for (UserNode un : followedList) {
		    HashMap<String, Object> userMap = pd.readRow(un.uid, new String[] {"name"},
		    		new int[] {pd.COLUMN_TYPE_STRING});
		    
	    	System.out.println("name: " + userMap.get("name") + "\tgender: " + un.gender + "\tage: " + un.age);
	    }
	}
}
