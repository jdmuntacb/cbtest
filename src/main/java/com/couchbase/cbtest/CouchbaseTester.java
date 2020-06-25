package com.couchbase.cbtest;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.diagnostics.PingResult;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.manager.collection.AsyncCollectionManager;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.query.QueryResult;



/**
 * 
 * A Couchbase DB testing tool
 * 
 *  @author jagadeshmunta
 *
 */
public class CouchbaseTester 
{
	
	static Logger logger = Logger.getLogger("cbtest");
	static Properties settings = null;
	static Cluster cluster = null;
	static Bucket bucket = null;
	
    public static void main( String[] args )
    {
        print("*** Couchbase Tester ***");
        CouchbaseTester cbtest = new CouchbaseTester(System.getProperties());
        String actions = settings.getProperty("run", "connectCluster");
        
    	StringTokenizer st = new StringTokenizer(actions, ",");
    	while (st.hasMoreTokens()) {
    		try {
    			String action = st.nextToken();
    			print("-->Running "+action);
    			if (action.lastIndexOf(".")!=-1) {
    				String clsName = action.substring(0,action.lastIndexOf("."));
    				String methodName = action.substring(action.lastIndexOf(".")+1);
    				print("-->Running "+clsName+" invoking "+methodName);
    				ClassLoader classLoader = cbtest.getClass().getClassLoader();
    				Object newObject = classLoader.loadClass(clsName).getDeclaredConstructor()
                            .newInstance();
    				Method method = newObject.getClass().getMethod(methodName, Properties.class);
    		        method.setAccessible(true);
    		        method.invoke(newObject,settings);
    			} else {
	    			Method method = cbtest.getClass().getMethod(action, Properties.class);
			        method.setAccessible(true);
			        method.invoke(cbtest,settings);
    			}
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }
    	}
    }
    
    public CouchbaseTester() {
    	this.settings = System.getProperties();
    }
    
    public CouchbaseTester(Properties props) {
    	this.settings = props;
    }
    public Cluster connectCluster(Properties props) {
    	String url = props.getProperty("url", "localhost");
    	String user = props.getProperty("user","Administrator");
    	String pwd = props.getProperty("password", "password");
    	String bucketName = props.getProperty("bucket","default");
    	boolean isdbaas = Boolean.parseBoolean(props.getProperty("dbaas", "false"));
    	
    	ClusterEnvironment env = ClusterEnvironment.builder()
    		       .ioConfig(IoConfig.enableDnsSrv(isdbaas))
    		       .securityConfig(SecurityConfig.enableTls(isdbaas)
    		               .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE))
    		       .build();
    	print("Connecting to cluster");
		cluster = Cluster.connect(url, 
				ClusterOptions.clusterOptions(user, pwd).environment(env));
		
		bucket = cluster.bucket(bucketName);
		bucket.waitUntilReady(Duration.parse("PT10S"));
		print("Connected to cluster");
		Collection collection = bucket.defaultCollection();
	
		return cluster;
    	
    }
    
    public boolean createBuckets(Properties props) {
    	boolean status = true;
    	String name = props.getProperty("bucket","default");
    	int count = Integer.parseInt(props.getProperty("bucket.count","1"));
    	    	
    	print("Creating buckets "+ name + ", count="+count);
    	
    	return status;
    }

    public void createScopes(Properties props) {
    	boolean status = true;
       	String bucketName = props.getProperty("bucket","default");
    	String scopeName = props.getProperty("scope","_default");
    	int count = Integer.parseInt(props.getProperty("scope.count","1"));
    	int start = Integer.parseInt(props.getProperty("scope.start","1"));
    	String operation = props.getProperty("operation","create");
    	print(operation+" scopes "+ scopeName + ", count="+count);
    	
    	long secsExp = Long.parseLong(props.getProperty("maxExpiry","-1"));
    	
    	Duration maxExpiry = null;
    	if (secsExp!=-1) {
    		maxExpiry = Duration.ofSeconds(secsExp);
    	} else {
    		maxExpiry = Duration.ZERO;
    	}
    	
    	if (count==1) {
    		print(operation+" <"+bucket.name()+"> scope: "+scopeName);
	    	ScopeSpec scopeSpec = ScopeSpec.create(scopeName);
	    	CollectionManager cm = bucket.collections();
	    	switch (operation) {
	    		case "create":
	    			cm.createScope(scopeName);
	    			break;
	    		case "drop":	
	    			cm.dropScope(scopeName);
	    			break;
	    		default:
	    			cm.createScope(scopeName);
	    	}
		} else {
	    	print(operation+" scopes "+ scopeName + "_xxx, count="+count);
	    	String sName = null;
	    	for (int i=start; i<count+start; i++) {
	    		if (count!=1) {
	    			sName = scopeName + "_"+i;
	    		} else {
	    			sName = scopeName + "_"+i;
	    		}
	    		print(operation+" <"+bucket.name()+"> scope: "+sName);
		    	ScopeSpec scopeSpec = ScopeSpec.create(sName);
		    	CollectionManager cm = bucket.collections();
		    	switch (operation) {
		    		case "create":
		    			cm.createScope(sName);
		    			break;
		    		case "drop":	
		    			cm.dropScope(sName);
		    			break;
		    		default:
		    			cm.createScope(sName);
		    	}
		    	
	    	}
		}
    	print("Done");
    	
    }

    public void createCollections(Properties props) {
    	boolean status = true;
       	String bucketName = props.getProperty("bucket","default");
    	String scopeName = props.getProperty("scope","_default");
    	String collectionName = props.getProperty("collection","_default");
    	int scopeCount = Integer.parseInt(props.getProperty("scope.count","1"));
    	int scopeStart = Integer.parseInt(props.getProperty("scope.start","1"));
    	int collectionCount = Integer.parseInt(props.getProperty("collection.count","1"));
    	int collectionStart = Integer.parseInt(props.getProperty("collection.start","1"));
    	long secsExp = Long.parseLong(props.getProperty("maxExpiry","-1"));
    	String operation = props.getProperty("operation","create");
    	
    	Duration maxExpiry = null;
    	if (secsExp!=-1) {
    		maxExpiry = Duration.ofSeconds(secsExp);
    	} else {
    		maxExpiry = Duration.ZERO;
    	}
    	
    	Scope scope = null;
    	if ("_default".contentEquals(scopeName)){
        	scope = bucket.defaultScope();
        } else {
        	scope = bucket.scope(scopeName);
        }
    	
    	if (collectionCount==1) {
    		print(operation+" <"+bucket.name()+"."+scope.name()+"> collection: "+collectionName);
	    	CollectionSpec collectionSpec = CollectionSpec.create(collectionName, scope.name(), maxExpiry);
	    	CollectionManager cm = bucket.collections();
	    	try {
	    		switch (operation) {
		    		case "create":
		    			if ("_default".equals(collectionName)) {
		    				bucket.defaultCollection();
		    			}
		    			cm.createCollection(collectionSpec);
		    			break;
		    		case "drop":	
		    			try {
		    				cm.dropCollection(collectionSpec);
		    			} catch (CollectionNotFoundException cnfe) {
		    				print(cnfe.getMessage());
		    			}
		    			break;
		    		case "dropall":
	    				for (CollectionSpec cs: cm.getScope(scope.name()).collections()) {
	    	        		print(operation+" collection "+bucket.name()+"."+scope.name()+"."+cs.name());
	    	        		try {
	    	    				cm.dropCollection(cs);
	    	    			} catch (CollectionNotFoundException cnfe) {
	    	    				print(cnfe.getMessage());
	    	    			}
	    	        		
	    	        	}
		        	    break;    
		    			
		    		default:
		    			cm.createCollection(collectionSpec);
	    		}
	    	}catch (CollectionExistsException cee) {
	    		print(cee.getMessage());
	    	}
    	} else if (scopeCount==1){
    		print(operation+" collections "+ collectionName + "_xxx, count="+collectionCount);
	    	String cName = null;
	    	
	    	if ("dropall".contentEquals(operation)) {
    			Collection collection = null;
    	        CollectionManager cm = bucket.collections();
    	        int cindex=0;
    	        
	        	for (CollectionSpec cs: cm.getScope(scope.name()).collections()) {
	        		print(operation+" collection "+bucket.name()+"."+scope.name()+"."+cs.name());
	        		try {
	    				cm.dropCollection(cs);
	    			} catch (CollectionNotFoundException cnfe) {
	    				print(cnfe.getMessage());
	    			}
	        		cindex++;
	        	}
    	        
			} else {
	    	
		    	for (int collectionIndex=collectionStart; collectionIndex<collectionCount+collectionStart; collectionIndex++) {
		    		cName = collectionName + "_"+collectionIndex;
		    		print(operation+" <"+bucket.name()+"."+scope.name()+"> collection: "+cName);
			    	CollectionSpec collectionSpec = CollectionSpec.create(cName, scope.name(), maxExpiry);
			    	CollectionManager cm = bucket.collections();
			    	try {
			    		switch (operation) {
				    		case "create":
				    			cm.createCollection(collectionSpec);
				    			break;
				    		case "drop":	
				    			try {
				    				cm.dropCollection(collectionSpec);
				    			} catch (CollectionNotFoundException cnfe) {
				    				print(cnfe.getMessage());
				    			}
				    			break;
				    		default:
				    			cm.createCollection(collectionSpec);
			    		}
			    	}catch (CollectionExistsException cee) {
			    		print(cee.getMessage());
			    	}
		    	}
			}
    	} else {
    		String sName = null;
    		for (int scopeIndex=scopeStart; scopeIndex<scopeCount+scopeStart; scopeIndex++) {
            	sName = scopeName +"_"+scopeIndex;
            	print(operation+" collections "+ collectionName + "_xxx, count="+collectionCount);
		    	String cName = null;
		    	
    			if ("dropall".contentEquals(operation)) {
	    			Collection collection = null;
	    	        CollectionManager cm = bucket.collections();
	    	        int cindex=0;
	    	        
    	        	for (CollectionSpec cs: cm.getScope(sName).collections()) {
    	        		print(operation+" collection "+bucket.name()+"."+sName+"."+cs.name());
    	        		try {
		    				cm.dropCollection(cs);
		    			} catch (CollectionNotFoundException cnfe) {
		    				print(cnfe.getMessage());
		    			}
    	        		cindex++;
    	        	}
	    	        
    			} else {
		    	
			    	for (int collectionIndex=collectionStart; collectionIndex<collectionCount+collectionStart; collectionIndex++) {
			    		cName = collectionName + "_"+collectionIndex;
			    		print(operation+" <"+bucket.name()+"."+sName+"> collection: "+cName);
				    	CollectionSpec collectionSpec = CollectionSpec.create(cName, sName, maxExpiry);
				    	CollectionManager cm = bucket.collections();
				    	try {
				    		switch (operation) {
					    		case "create":
					    			cm.createCollection(collectionSpec);
					    			break;
					    		case "drop":	
					    			try {
					    				cm.dropCollection(collectionSpec);
					    			} catch (CollectionNotFoundException cnfe) {
					    				print(cnfe.getMessage());
					    			}
					    			break;
					    		
					    		default:
					    			cm.createCollection(collectionSpec);
					    			break;
				    		}
				    		
				    	}catch (CollectionExistsException cee) {
				    		print(cee.getMessage());
				    	}
			    	}
    			}
    		}
    	}
    	
    	print("Done");
    }
    
    public void listCollections(Properties props) {
    	String bucketName = props.getProperty("bucket","default");
    	String scopeName = props.getProperty("scope","_default");
    	
        Bucket bucket = cluster.bucket(bucketName);
        Collection collection = null;
        CollectionManager cm = bucket.collections();
        int sindex=0, cindex=0;
        for (ScopeSpec s: cm.getAllScopes()) {
        	sindex++;
        	for (CollectionSpec cs: s.collections()) {
        		print(bucket.name()+"."+s.name()+"."+cs.name());
        		cindex++;
        	}
        }
        print("Scopes: "+sindex+", Collections: "+cindex);
        
    }
    
    public void countCollections(Properties props) {
    	String bucketName = props.getProperty("bucket","default");
    	String scopeName = props.getProperty("scope","_default");
    	
        Bucket bucket = cluster.bucket(bucketName);
        Collection collection = null;
        CollectionManager cm = bucket.collections();
        int scopesCount = cm.getAllScopes().size();
        int collectionsCount = 0;
        for (ScopeSpec s: cm.getAllScopes()) {
        	collectionsCount += s.collections().size();
        }
        print("Scopes: "+scopesCount+", Collections: "+collectionsCount);
        
    }
    
    public void createDocs(Properties props) {
    	String bucketName = props.getProperty("bucket","default");
    	String scopeName = props.getProperty("scope","_default");
    	String collectionName = props.getProperty("collection","_default");
    	String docId = props.getProperty("doc.id","my_document");
    	String docData = props.getProperty("doc","_default");
    	int docCount = Integer.parseInt(props.getProperty("doc.count","1"));
    	int docStart = Integer.parseInt(props.getProperty("doc.start","1"));
    	String operation = props.getProperty("operation","create");
    	
    	
        Bucket bucket = cluster.bucket(bucketName);
        Collection collection = null;
        
        if (("_default".contentEquals(scopeName)) && ("_default".contentEquals(collectionName))) {
        	collection = bucket.defaultCollection();
        } else if ("_default".contentEquals(scopeName)){
        	collection = bucket.defaultScope().collection(collectionName);
        } else {
        	collection = bucket.scope(scopeName).collection(collectionName);
        }

        if (docCount==1) {
	        JsonObject json = null;
	        if ("_default".contentEquals(docData)) {
	        	json = JsonObject.create().put("name", System.getProperty("user.name"));
	        } else {
	        	json = JsonObject.fromJson(docData);
	        }
	        print(operation+" doc at <"+bucket.name()+"."+scopeName+"."+collectionName +"> with " + docId+":"+json);
	        MutationResult result = null;
            GetResult getResult = null;
            switch (operation) {
	    		case "create":
	    			result = collection.upsert( docId, json);
		            getResult = collection.get(docId);
		            print(getResult.toString());
		            break;
	    		case "drop":	
	    			result = collection.remove(docId);
	    			break;
	    		default:
	    			result = collection.upsert( docId, json);
		            getResult = collection.get(docId);
		            print(getResult.toString());
    		}
        } else {
	        String docKey = null;
	        for (int docIndex=docStart; docIndex<docCount+docStart; docIndex++) {
	        	JsonObject json = null;
	            if ("_default".contentEquals(docData)) {
	            	json = JsonObject.create().put("name", System.getProperty("user.name")).put("index", docIndex);
	            } else {
	            	json = JsonObject.fromJson(docData);
	            	json.put("index", docIndex);
	            }
	            docKey = docId+"_"+docIndex;
	            print(operation+" doc at <"+bucket.name()+"."+scopeName+"."+collectionName +"> with " + docKey+":"+json);
	            MutationResult result = null;
	            GetResult getResult = null;
	            switch (operation) {
		    		case "create":
		    			result = collection.upsert( docKey, json);
			            getResult = collection.get(docKey);
			            print(getResult.toString());
			            break;
		    		case "drop":	
		    			try {
		    				result = collection.remove(docKey);
		    			} catch (DocumentNotFoundException dnf) {
		    				print(dnf.getMessage());
		    			}
		    			break;
		    		default:
		    			result = collection.upsert( docKey, json);
			            getResult = collection.get(docKey);
			            print(getResult.toString());
	    		}
	        }
        }
        
       
    }
    
    public void createTenantDocs(Properties props) {
    	String bucketName = props.getProperty("bucket","default");
    	String scopeName = props.getProperty("scope","_default");
    	String collectionName = props.getProperty("collection","_default");
    	String docId = props.getProperty("doc.id","my_document");
    	String docData = props.getProperty("doc","_default");
    	int scopeCount = Integer.parseInt(props.getProperty("scope.count","1"));
    	int scopeStart = Integer.parseInt(props.getProperty("scope.start","1"));
    	int collectionCount = Integer.parseInt(props.getProperty("collection.count","1"));
    	int collectionStart = Integer.parseInt(props.getProperty("collection.start","1"));
    	int docCount = Integer.parseInt(props.getProperty("doc.count","1"));
    	int docStart = Integer.parseInt(props.getProperty("doc.start","1"));
    	String operation = props.getProperty("operation","create");
    	
    	
        Bucket bucket = cluster.bucket(bucketName);
        Collection collection = null;
        
        
        String sName = null, cName = null;
        for (int scopeIndex=scopeStart; scopeIndex<scopeCount+scopeStart; scopeIndex++) {
        	sName = scopeName +"_"+scopeIndex;
        	for (int collectionIndex=collectionStart; collectionIndex<collectionCount+collectionStart; collectionIndex++) {
		        cName = collectionName + "_"+collectionIndex;
        		String docKey = null;
		        for (int docIndex=docStart; docIndex<docCount+docStart; docIndex++) {
		        	JsonObject json = null;
		            if ("_default".contentEquals(docData)) {
		            	json = JsonObject.create().put("name", System.getProperty("user.name")).put("index", docIndex);
		            } else {
		            	json = JsonObject.fromJson(docData);
		            	json.put("index", docIndex);
		            }
		            docKey = docId+"_"+docIndex;
		            print(operation+" doc at <"+bucket.name()+"."+sName+"."+cName +"> with " + docKey+":"+json);
		            collection = bucket.scope(sName).collection(cName);
		            MutationResult result = null;
		            GetResult getResult = null;
		            switch (operation) {
			    		case "create":
			    			result = collection.upsert( docKey, json);
				            getResult = collection.get(docKey);
				            print(getResult.toString());
				            break;
			    		case "drop":	
			    			try {
			    				result = collection.remove(docKey);
			    			} catch (DocumentNotFoundException dnf) {
			    				print(dnf.getMessage());
			    			}
			    			break;
			    		
			    		default:
			    			result = collection.upsert( docKey, json);
				            getResult = collection.get(docKey);
				            print(getResult.toString());
		    		}
		            
		            
		           
		        }
        	}
        }
        
       
    }

    public void qryGreeting(Properties props) {
    	 QueryResult result = cluster.query("select \"Hello World\" as greeting");
         print(result.rowsAsObject().toString());
    }
    
    public void pingCluster(Properties props) {
    	PingResult ping = cluster.ping();
		print(ping.toString());
    }
    
    public static void print(String s) {
    	System.out.println(new Date() +" "+s);
    }
}
