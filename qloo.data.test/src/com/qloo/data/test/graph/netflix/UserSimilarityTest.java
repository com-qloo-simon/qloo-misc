package com.qloo.data.test.graph.netflix;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;

import com.qloo.data.graph.netflix.ChoiceNode;
import com.qloo.data.graph.netflix.UserNode;
import com.qloo.data.graph.similarity.UserSimilarityT;
import com.qloo.data.util.Cosine;
import com.qloo.data.util.Euclidean;
import com.qloo.data.util.Pearson;
import com.qloo.data.util.Pearson0;
import com.qloo.data.util.Pearson1;
import com.qloo.data.util.Similarity;


public abstract class UserSimilarityTest extends UserSimilarityT<UserNode, ChoiceNode> {
	protected Similarity<ChoiceNode> similarity = null;
	
	public UserSimilarityTest(Similarity<ChoiceNode> similarity) {
		//this.similarity = similarity;
		super(similarity);
	}
	
	// public abstract void calculateUserSimilarityMap(final UserNode un, final Map<UserNode, Double>[] similarityMapArray, final Map<ChoiceNode, Double>[] likeMap0Array, final Map<ChoiceNode, Double>[] likeMapArray);
	
	public static void main(String args[]) {
        try {
			BufferedReader br = new BufferedReader(new FileReader(args[0]));

			// Read each line of text in the file
			HashMap<Integer, HashMap<Integer, Double>> mapTotal = new HashMap<Integer, HashMap<Integer, Double>>();                
                       
			int currentUserID = 0;
			HashMap<Integer, Double> currentMap = new HashMap<Integer, Double>();      
			mapTotal.put(0, currentMap);
            
			String line = null;
            while ((line = br.readLine()) != null) {
				String[] data=line.split(",");
				
				int userID = Integer.parseInt(data[0]);
				int itemID = Integer.parseInt(data[1]);
				double rate = Double.parseDouble(data[2]);
                        
				//if (userID == 0) System.out.println("userID: " + userID + "\titemID: " + itemID + "\trate: " + rate);
                        
				if (userID == currentUserID) {
					currentMap.put(itemID, rate);
				} else {
					 currentMap = new HashMap<Integer, Double>();
					 currentMap.put(itemID, rate);
					 
					 mapTotal.put(userID, currentMap);

					 currentUserID = userID;
				}
            }
                       
            br.close();

			Similarity<Integer> s = null;
			
			if ("cosine".equals(args[2])) {
				s = new Cosine<Integer>();
			} else if ("euclidean".equals(args[2])) {
				s = new Euclidean<Integer>();
			} else if ("pearson".equals(args[2])) {
				s = new Pearson<Integer>();
			} else if ("pearson0".equals(args[2])) {
				s = new Pearson0<Integer>();
			} else if ("pearson1".equals(args[2])) {
				s = new Pearson1<Integer>();
			}

			double minSim = 0, maxSim = 0;
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(args[1]));
			StringBuffer sb = new StringBuffer();
			
			for (int j = 0; j <= currentUserID; j++) {
				HashMap<Integer, Double> map0 = mapTotal.get(j);
				if (map0 == null) continue;

				for (int i = j + 1; i <= currentUserID && i != j; i++) {
					HashMap<Integer, Double> map = mapTotal.get(i);
					if (map != null) {
						double sim = s.getSimilarity(map0, map);
						if (sim < minSim) minSim = sim;
						if (sim > maxSim) maxSim = sim;
						
						sb.setLength(0);
						sb.append(j); sb.append(','); sb.append(i); sb.append(',');
						sb.append(sim); sb.append('\n');
						bw.write(sb.toString());
					}
				} 
			}
			
			bw.close();
			
			System.out.println("min similarity:" + minSim + "\tmax similarity: " + maxSim);
		} catch (Throwable th) {
			th.printStackTrace();
		}
	}
}
