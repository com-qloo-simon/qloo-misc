package com.qloo.data.test.dao.astyanax;

import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.Keyspace;
import com.qloo.data.cassandra.KSFactory;
import com.qloo.data.cassandra.LikedDAO;
import com.qloo.data.cassandra.UserColumn;


public class LikedDAOTest {
	static Keyspace ks;
	static LikedDAO ldd;

	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		ks = KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3");

		ldd = new LikedDAO();
	    ldd.init(ks, "liked");
    }
 
	@Test
	public void load() {
		System.out.println("@Test - load");
		
	    ldd.load(ks, "qloo");
	}
	
	@Test
	public void readRow() {
		System.out.println("@Test - readRow");
		
	    List<UserColumn> ucs = ldd.readRow(UUID.fromString("d98567fe-688b-4f83-8fd3-fec9888d4b68"));
	    
	    for (UserColumn uc : ucs)
	       System.out.println("uid: " + uc.uid + "\tgender: " + uc.gender);    
	    
	    //li.writeLiked(UUID.fromString("d98567fe-688b-4f83-8fd3-fec9888d4b68"), UUID.fromString("9b92e899-24b3-43fa-9a5f-55ecebf73cfb"), 0.6f);
	    
	    //li.removeFollowed(UUID.fromString("e83e8895-c24c-4cff-b816-c503acaf8394"), UUID.fromString("2f6251af-96ac-4a1e-bfe3-295936ba2605"));
	}
}
