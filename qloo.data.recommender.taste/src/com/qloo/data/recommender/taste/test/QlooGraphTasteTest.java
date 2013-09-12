package com.qloo.data.recommender.taste.test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.astyanax.Keyspace;

import com.qloo.data.cassandra.CategoryDAO;
import com.qloo.data.cassandra.ChoiceDAO;
import com.qloo.data.cassandra.KSFactory;
import com.qloo.data.cassandra.ProfileDAO;
import com.qloo.data.graph.netflix.ChoiceNode;
import com.qloo.data.graph.netflix.QlooGraphDAO;
import com.qloo.data.graph.netflix.UserNode;


public class QlooGraphTasteTest {
	static DataModel model;
	
	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		long lStart = System.currentTimeMillis();
		
		try {
			model = new GenericBooleanPrefDataModel(new FileDataModel(new File("/Users/qloo/work/test.csv")));
		} catch (TasteException te) {
			te.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (NoSuchElementException nse) {
			nse.printStackTrace();
		}
		
		System.out.println("setup takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
	}
	
	@Test
	public void testGenericBooleanPrefUserBasedRecommender() {
		System.out.println("@Test - testGenericBooleanPrefUserBasedRecommender");
		
		long lStart = System.currentTimeMillis();
		
		try {	
			// UserSimilarity userSimilarity = new PearsonCorrelationSimilarity(model);
			UserSimilarity userSimilarity = new LogLikelihoodSimilarity(model);
			
			// userSimilarity.setPreferenceInferrer(new AveragingPreferenceInferrer(model));
			
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(10, 0.7, userSimilarity, model);
			
			// Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, userSimilarity);
			// Recommender recommender = new SlopeOneRecommender(model);
			Recommender recommender = new GenericBooleanPrefUserBasedRecommender(model, neighborhood, userSimilarity);
			
			Recommender cachingRecommender = new CachingRecommender(recommender);
			
			model.getPreferencesFromUser(1);
			
			
			long lEnd = System.currentTimeMillis();
			System.out.println("building takes " + (lEnd - lStart) + " mill seconds");
			lStart = lEnd;
			
			List<RecommendedItem> recommendations = cachingRecommender.recommend(1, 10);
			
			System.out.println(recommendations.size());
			
			System.out.println("recommendation takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
			
			final QlooGraphDAO qgd = new QlooGraphDAO();
			
			try {
				qgd.input("/Users/qloo/work/test.out");
		    } catch (IOException ioe) {
		    	ioe.printStackTrace();
		    	return;
		    }
			
			UserNode un = qgd.userOrdinals.get(1);
			
			Keyspace ks = KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3");
			
			final ProfileDAO pd = new ProfileDAO();
		    pd.init(ks, "profile");
		    
		    HashMap<String, Object> userMap = pd.readRow(un.uid, new String[] {"uname", "name"},
		    		new int[] {pd.COLUMN_TYPE_STRING, pd.COLUMN_TYPE_STRING});
		    
		    System.out.println("uname: " + userMap.get("uname") + "\tname: " + userMap.get("name") + "\tgender: " + un.gender + "\tage: " + un.age);
			
		    final ChoiceDAO cd = new ChoiceDAO();
		    cd.init(ks, "choice_used");
		    
		    final CategoryDAO ci = new CategoryDAO();
		    ci.init(ks, "category");
		    
			for (RecommendedItem item : recommendations) {
				ChoiceNode cn = qgd.choiceOrdinals.get((int)item.getItemID());
				
				HashMap<String, Object> choiceMap = cd.readRow(cn.cid, new String[] {"name"},
			    		new int[] {cd.COLUMN_TYPE_STRING});
				
				//HashMap<String, Object> categoryMap = ci.readRow(cn.gid, new String[] {"name"},
			    //		new int[] {cd.COLUMN_TYPE_STRING});
				
				System.out.println("score: " + item.getValue() + "\tcategory sn: " + cn.giid + "\tname: " + choiceMap.get("name"));
			}
		} catch (TasteException te) {
			te.printStackTrace();
		}
	}
}
