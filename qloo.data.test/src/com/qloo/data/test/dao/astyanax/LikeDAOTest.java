package com.qloo.data.test.dao.astyanax;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.netflix.astyanax.Keyspace;
import com.qloo.data.cassandra.ChoiceColumn;
import com.qloo.data.cassandra.KSFactory;
import com.qloo.data.cassandra.LikeDAO;
import com.qloo.data.cassandra.LikedDAO;
import com.qloo.data.cassandra.LikedObj;


public class LikeDAOTest {
	static Keyspace ks;
	static LikeDAO ld;

	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		ks = KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3");

		ld = new LikeDAO();
	    ld.init(ks, "like");
    }
	
	@Ignore("Not Ready to Run")
	@Test
	public void load() {
		System.out.println("@Test - load");
		
		ld.load(ks, "qloo");
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void printSimilarUsers() {
		System.out.println("@Test - printSimilarUsers");
		
		final LikedDAO ldd = new LikedDAO();
	    ldd.init(ks, "liked");
	    
	    long lStart = System.currentTimeMillis();
	    
	    ChoiceColumn[] los = ld.readRow(UUID.fromString("281EFE6F-3BCF-4302-A66F-63D265F3461A"));
	    
	    System.out.println("like count: " + los.length);
	    
	    long lEnd = System.currentTimeMillis();
	    System.out.println(lEnd - lStart);
	    lStart = lEnd;
	    
	    UUID[] cids = new UUID[los.length];
	    for (int i = 0; i < cids.length ; i++)
	    	cids[i] = los[i].cid;
	    
	    List<LikedObj> list = ldd.readRows(cids);
	    
	    System.out.println("total simiar users " + list.size());
	    
	    System.out.println(System.currentTimeMillis() - lStart);
	}
	
	@Test
	public void printSimilarUserMap() {
		System.out.println("@Test - printSimilarUserMap");
	    
	    long lStart = System.currentTimeMillis();
	    
	    Map<UUID, Double> map = ld.getChoiceUUIDLikeMap(UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2"));
	    
	    System.out.println("similarity count: " + map.size() + "\ttakes: " + (System.currentTimeMillis() - lStart) + " milli seconds");
	    
	    for (Map.Entry<UUID, Double> entry : map.entrySet()) {
	    	System.out.println("uid: " + entry.getKey() + "\tvalue: " + entry.getValue());
	    }
	}
}
