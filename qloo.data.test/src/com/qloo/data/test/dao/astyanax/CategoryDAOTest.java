package com.qloo.data.test.dao.astyanax;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.qloo.data.cassandra.CategoryDAO;
import com.qloo.data.cassandra.KSFactory;


public class CategoryDAOTest {
	static CategoryDAO cd;
	
	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		cd = new CategoryDAO();
    	cd.init(KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3"), "category");
    }
 
    @AfterClass
    public static void oneTimeTearDown() {
        // one-time cleanup code
    	System.out.println("@AfterClass - oneTimeTearDown");
    }
    
    @Before
    public void setUp() {
        System.out.println("@Before - setUp");
    }
    
    @After
    public void tearDown() {
        System.out.println("@After - tearDown");
    }
	
    @Ignore("Not Ready to Run")
	@Test
	public void load() {
		System.out.println("@Test - load");
		
		cd.load(KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "baldr"), "category");
	}
	
	@Test
	public void readAllCategoryInfoMap() {
		System.out.println("@Test - readAllCategoryInfoMap");
		
	    Map<UUID, CategoryDAO.CategoryInfo> map = cd.readAllCategoryInfoMap();
	    for (Map.Entry<UUID, CategoryDAO.CategoryInfo> entry : map.entrySet()) {
	    	System.out.println("id: " + entry.getKey() + "\tiid: " + entry.getValue().iid + "\tname: " + entry.getValue().name + "\tpiid: " + entry.getValue().piid);
	    }
	}

	@Ignore("Not Ready to Run")
	@Test
	public void testOthers() {
		System.out.println("@Test - testOthers");
		
	    // ci.writeColumn(UUID.fromString("e0ccf3be-0e92-4860-82ad-12eab3bd6113"), "pname", null);
	    // ci.deleteColumn(UUID.fromString("e0ccf3be-0e92-4860-82ad-12eab3bd6113"), "pname");
	    
	    /*
	    HashMap<String, Object> map = new HashMap<String, Object>();
	    map.put("pid", UUID.fromString("e0ccf3be-0e92-4860-82ad-12eab3bd6113"));
	    map.put("pname", "test");
	    
	    ci.writeColumns(UUID.fromString("e0ccf3be-0e92-4860-82ad-12eab3bd6113"), map);*/
	    
	    // ci.deleteColumns(UUID.fromString("e0ccf3be-0e92-4860-82ad-12eab3bd6113"), "pid", "pname");
	    
	    // ci.deleteRow(UUID.fromString("e0ccf3be-0e92-4860-82ad-12eab3bd6113"));
	    
	    // ci.deleteRows(UUID.fromString("8c89e821-eded-49ff-9b8d-99d77172fde3"), UUID.fromString("eff7e668-41ee-4dbb-8777-a7bcdac2f659"));
	
	    /*
	    UUID id = UUID.fromString("1a7a9b3f-bfa4-4a7a-94f4-01c8a56a4a8e");
	    
	    System.out.println(ci.readColumn(id, "name", ci.COLUMN_TYPE_STRING));	    
	    
	    HashMap<String, Object> map = ci.readColumns(id, 
	    		new String[] {"pid", "pname", "visible"},
	    		new int[] {ci.COLUMN_TYPE_UUID, ci.COLUMN_TYPE_STRING, ci.COLUMN_TYPE_BOOLEAN});
	    
	    System.out.println(map.size());
	    
	    for (Map.Entry<String, Object> entry : map.entrySet()) {
	    	System.out.print("-col '" + entry.getKey() + "' : " + entry.getValue() + "\t");
	    }
	    
	    System.out.println();
	    */
	    
	   // System.out.println(ci.readColumn(UUID.fromString("8fd95deb-3850-4d64-b53d-7c7c748c9b05"), "pid", ci.COLUMN_TYPE_UUID));	
	   
	    /*
	   HashMap<UUID, HashMap<String, Object>> maps = ci.readAllColumnsToMap(
	    		new String[] {"pid", "pname", "visible"},
	    		new int[] {ci.COLUMN_TYPE_UUID, ci.COLUMN_TYPE_STRING, ci.COLUMN_TYPE_BOOLEAN});
	    
	   for (Map.Entry<UUID, HashMap<String, Object>> entry : maps.entrySet()) {
	    	System.out.print("row key: " + entry.getKey() + "\t");
	    	
	    	for (Map.Entry<String, Object> entry2 : entry.getValue().entrySet()) {
	    		System.out.print("-col '" + entry2.getKey() + "' : " + entry2.getValue() + "\t");
	    	}
	    	
	    	System.out.println();
	    }*/
	   
	    /*
		HashMap<UUID, HashMap<String, Object>> maps = ci.readAllColumnsToMap(
    		new String[] {"name", "pid"},
    		new int[] {ci.COLUMN_TYPE_STRING, ci.COLUMN_TYPE_UUID});
    
   		for (Map.Entry<UUID, HashMap<String, Object>> entry : maps.entrySet()) {
    		System.out.print("row key: " + entry.getKey() + "\t");
    	
    	for (Map.Entry<String, Object> entry2 : entry.getValue().entrySet()) {
    		System.out.print("-col '" + entry2.getKey() + "' : " + entry2.getValue() + "\t");
    	}
    	
    	System.out.println();
    	}*/
	}
}
