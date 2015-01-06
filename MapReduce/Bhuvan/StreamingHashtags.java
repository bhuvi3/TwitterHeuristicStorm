
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import twitter4j.FilterQuery;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;


public class StreamingHashtags {
	/**
	* Main entry of this application.
	*
	* @param args
	     * @throws IOException 
	*/
		static BufferedWriter textfilewriter,jsonfilewriter,tweetWriter;
		static int i=0;
		public static HashSet<String> cities=new HashSet<String>(){
			@Override
		    public boolean contains(Object o) {
		        String paramStr = (String)o;
		        for (String s : this) {
		            if (paramStr.equalsIgnoreCase(s)) return true;
		        }
		        return false;
		    }
		};
	    public static void main(String[] args) throws TwitterException, IOException {
	    	ConfigurationBuilder cb = new ConfigurationBuilder();
			cb.setDebugEnabled(true)
			  .setOAuthConsumerKey("KtiWruV6KfQeJ3uHmT4I2w")
			  .setOAuthConsumerSecret("8m0R4vmVic9vYVRizVcvFOyQPVTuwIiUZcpLDtbNchg")
			  .setOAuthAccessToken("66720028-r8gnJxYFhm0WBWwXoEChMmjhTfEKMiohDTHDpNg11")
			  .setOAuthAccessTokenSecret("RWxdMySOinNVQ0pzdAxaRuk0um2OruyBBv4ihoDr4")
			  .setJSONStoreEnabled(true);
	        TwitterStream tweetStream = new TwitterStreamFactory(cb.build()).getInstance();
	        File textfile = new File(new String("C:\\Users\\Bhuvan\\workspace\\TwitterHeuristicsStorm\\statusfile.txt"));
	        if (!textfile.exists())
	        	textfile.createNewFile();
	        File jsonfile = new File(new String("C:\\Users\\Bhuvan\\workspace\\TwitterHeuristicsStorm\\statusfile.json"));
	        if (!jsonfile.exists())
	        	jsonfile.createNewFile();
	        textfilewriter = new BufferedWriter(new FileWriter(textfile.getAbsoluteFile(),true));
	        jsonfilewriter = new BufferedWriter(new FileWriter(jsonfile.getAbsoluteFile(),true));
	        tweetWriter =new BufferedWriter(new FileWriter("C:\\Users\\Bhuvan\\workspace\\TwitterHeuristicsStorm\\City_tweets.txt"));
	        StatusListener listener = new StatusListener() {
	        	
	            public void onStatus(Status status) {
	            	
	            	if(cities.contains(status.getUser().getLocation())){
	            		try {
							tweetWriter.write(status.getUser().getLocation()+":"+status.getText()+"\n");
							i++;
							System.out.println(i);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
	            	}
	            	if(i>=100){
	            		try{
	            			tweetWriter.close();
	            			System.out.println("100 Status retrieved!!\nProgram Terminating!!");
	    					System.exit(1);
	            		}catch(Exception e){
	            			System.out.println("Exiting Problem!!!");
	            			e.printStackTrace();
	            		}
	            	}
	            	//System.out.println("*********\n"+status.getUser().getLocation()+";"+status.getText()+"\n**********");
	            	/*
	            	HashtagEntity hash[]= status.getHashtagEntities();
	            		for (int j=0;j<hash.length;j++)
	            		{
	            			try {
	            					textfilewriter.write(hash[j].getText()+"\n");
	            			} catch (IOException e) {
	            				// TODO Auto-generated catch block
	            				System.out.println("Error in text file write");
	            				e.printStackTrace();
	            			}
	            		}
	            	String jsonstr = DataObjectFactory.getRawJSON(status);
	            	//JSONObject j_ob = (JSONObject) JSONSerializer.toJSON(status);
	            	try {
	    				if(i<40)
	    				{
	    					jsonfilewriter.write(jsonstr+"\n");
	    					i++;
	    				}
	    				else
	    				{
	    					jsonfilewriter.close();
	    					textfilewriter.close();
	    					System.out.println("100 Status retrieved!!\nProgram Terminating!!");
	    					System.exit(1);
	    				}
	    			} catch (IOException e) {
	    				// TODO Auto-generated catch block
	    				System.out.println("Error in json file write");
	    				e.printStackTrace();
	    			}
	            */	
	            }
	            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
	                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
	            }

	            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
	                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
	            }

	            public void onScrubGeo(long userId, long upToStatusId) {
	                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
	            }

	            public void onStallWarning(StallWarning warning) {
	                System.out.println("Got stall warning:" + warning);
	            }

	            public void onException(Exception ex) {
	                ex.printStackTrace();
	            }
	        };
	        loadCityDict();
	        tweetStream.addListener(listener);
	        FilterQuery fq = new FilterQuery();
		    String keywords[] = {"Rahul Gandhi","RahulGandhi","rahulgandhi","rahul gandhi","PappuCII","FekuAlert","NAMO","namo","Narendra Modi","NarendraModi","narendra modi","feku","Feku","ModifyDelhi","RahulVsArnab","RahulSpeaksToArnab","rahulSpeaksToArnab"};
		    tweetStream.filter(fq);
		    fq.track(keywords);
	        //tweetStream.sample();
	    }
	    private static void loadCityDict(){
	    	FileReader fr;
	    	BufferedReader br;
	    	try{
	    		fr=new FileReader("C:\\Users\\Bhuvan\\workspace\\TwitterHeuristicsStorm\\CityDict.txt");
	    		br=new BufferedReader(fr);
	    		String city;
	    		while((city=br.readLine()) != null){
	    			cities.add(city);
	    		}
	    		br.close();
	    	}catch(Exception e){
	    		e.printStackTrace();
	    	}
	    }
}
