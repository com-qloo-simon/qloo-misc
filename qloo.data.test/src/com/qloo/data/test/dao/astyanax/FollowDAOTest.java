package com.qloo.data.test.dao.astyanax;

import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.qloo.data.cassandra.FollowDAO;
import com.qloo.data.cassandra.KSFactory;
import com.qloo.data.cassandra.UserColumn;


public class FollowDAOTest {
	static FollowDAO fd;
	
	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");

		fd = new FollowDAO();
	    fd.init(KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3"), "follow");
    }
	
	@Test
	public void load() {
		System.out.println("@Test - load");
		
	    fd.load(KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "baldr"), "following");
	}
	
	@Test
	public void readRow() {
		System.out.println("@Test - readRow");
		
		List<UserColumn> ucs = fd.readRow(UUID.fromString("afe381ce-7087-4d1a-91ab-98e63e1e84f4"));
	    
		for (UserColumn uc : ucs)
	       System.out.println("uid: " + uc.uid + "\tgender: " + uc.gender); 
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void testOthers() {
		System.out.println("@Test - testOthers");
		
	    // fi.writeFollow(UUID.fromString("afe381ce-7087-4d1a-91ab-98e63e1e84f4"), UUID.fromString("2f6251af-96ac-4a1e-bfe3-295936ba2605"), new Date());
	    
	    //fi.removeFollow(UUID.fromString("afe381ce-7087-4d1a-91ab-98e63e1e84f4"), UUID.fromString("2f6251af-96ac-4a1e-bfe3-295936ba2605"));
	}
}
