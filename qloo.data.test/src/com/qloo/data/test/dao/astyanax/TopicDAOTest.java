package com.qloo.data.test.dao.astyanax;

import java.util.Map;
import java.util.UUID;
import java.util.HashSet;

import org.junit.BeforeClass;
import org.junit.Test;

import com.qloo.data.cassandra.KSFactory;
import com.qloo.data.cassandra.TopicDAO;


public class TopicDAOTest {
	static TopicDAO td;
	
	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		td = new TopicDAO();
	    td.init(KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3"), "topic");
    }
	
	@Test
	public void load() {
		System.out.println("@Test - load");
		
		td.load(KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "baldr"), "topic");
	}
	
	@Test
	public void readColumn() {
		System.out.println("@Test - readColumn");
		
		System.out.println(td.readColumn(UUID.fromString("d2b46480-d422-4cd1-9b5b-1e024ac76e68"), "name", td.COLUMN_TYPE_STRING));
		System.out.println(td.readColumn(UUID.fromString("d2b46480-d422-4cd1-9b5b-1e024ac76e68"), "gid", td.COLUMN_TYPE_UUID));
	}
	
	@Test
	public void printAllTopicInfos() {
		System.out.println("@Test - printAllTopicInfos");
		
		Map<UUID, TopicDAO.TopicInfo> map = td.readAllTopicInfoMap();
		
		int count = 0; HashSet<Integer> hs = new HashSet<Integer>();
		for (Map.Entry<UUID, TopicDAO.TopicInfo> entry : map.entrySet()) {
			count++; hs.add(entry.getValue().iid);
	    	System.out.println("id: " + entry.getKey() + "\tiid: " + entry.getValue().iid + "\tgiid: " + entry.getValue().giid + "\tug: " + entry.getValue().ug);
	    }
		
		System.out.println("count: " + count + "\tunique iid: " + hs.size());
	}
}
