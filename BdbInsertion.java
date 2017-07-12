package Dim3;


import java.io.File;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.FileManager;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;

public class BdbInsertion {
	
       static final String fileName = "sampleDim3Normal.rdf";
	static final String filePath = "./sampledata";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		

		Model model = ModelFactory.createDefaultModel();
		
		///// The Parameters
		///// The Parameters
		///// The Parameters
		InputStream in = FileManager.get().open(fileName);
		if (in == null) {
			System.out.println("The file is not found");
		}
		model.read(in, "", "N-TRIPLE");
		// System.out.println("The following is the data in RDF file:");
		// model.write(System.out, "N-TRIPLE");

	
		// list the statements in the Model
		StmtIterator iter = model.listStatements();

		HashMap<String, Database> opendbMap = new HashMap<String, Database>();
		HashMap<String, SecondaryDatabase> openSecdbMap = new HashMap<String, SecondaryDatabase>();
		
		// create environment
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(false);

		///// The Parameters
		///// The Parameters
		///// The Parameters	
		Environment myDBenvironment = new Environment(new File(filePath), envConfig);

		

		int i = 0;
		while (iter.hasNext()) {
			com.hp.hpl.jena.rdf.model.Statement stmt = iter.nextStatement(); // get
			// next
			// statement
			Resource subject = stmt.getSubject(); // get the subject
			Property predicate = stmt.getPredicate(); // get the predicate
			RDFNode object = stmt.getObject(); // get the object

			String subjString = subject.toString();
			String[] splitSubArray = subjString.split("/");
			String splitSubj = splitSubArray[splitSubArray.length - 1] + splitSubArray[splitSubArray.length - 2]+ splitSubArray[splitSubArray.length - 3]+ splitSubArray[splitSubArray.length - 4] + splitSubArray[splitSubArray.length - 5];

			System.out.println("subject is: +++++++++++++++++: " + subjString);

			// get the predicate string
			String predString = predicate.toString();
			String[] splitPreArray = predString.split("/");
			String splitPre = splitPreArray[splitPreArray.length - 1];
			 System.out.println("property is: ++++++++++++: " +	 splitPre);

			// retrive object value
			String objString = object.toString();
//	//		int objectVal = ((Long.parseLong(objString)) / 100);
//			float longVal = (Long.parseLong(objString)) / 100;
//			String objectValString = String.valueOf(longVal);
//			String[] splitObjArray = predString.split(".");
//			String splitObj = splitObjArray[0];
			int objectVal = Integer.parseInt(objString);
			
			
		System.out.println("object is: ++++++++++++: " + objectVal);

			// map
			String dbname = splitPre + "db";
//			System.out.println("dbname is: " + dbname);
			String secdbname = splitPre + "SecDB";
//			System.out.println("secdbname is: " + secdbname);
			
	
			DatabaseConfig dbConfig = null;
			Database myDatabase = null;
			SecondaryConfig mySecConfig = null;
			SecondaryDatabase mySecondaryDatabase = null;

			try {
				if (opendbMap.containsKey(dbname)) {
					
					myDatabase = opendbMap.get(dbname);
					mySecondaryDatabase = openSecdbMap.get(dbname);
					
				} else {
					// create database
					
					dbConfig = new DatabaseConfig();
					dbConfig.setAllowCreate(true);
					dbConfig.setTransactional(false);
					myDatabase = myDBenvironment.openDatabase(null, dbname,
							dbConfig);

					// create secondary database
					mySecConfig = new SecondaryConfig();
					mySecConfig.setAllowCreate(true);
					mySecConfig.setSortedDuplicates(true);
					mySecConfig.setTransactional(false);
					mySecConfig.setKeyCreator(new SecondKeyCreator());
					mySecondaryDatabase = myDBenvironment
							.openSecondaryDatabase(null, secdbname, myDatabase,
									mySecConfig);
					
					opendbMap.put(dbname, myDatabase);
					openSecdbMap.put(dbname, mySecondaryDatabase);

					
				}

				// primary key
				DatabaseEntry myKey;

				myKey = new DatabaseEntry(splitSubj.getBytes("UTF-8"));

				// value and secondary key
				Record mydata = new Record();
				mydata.setobjectVal(objectVal);

				DatabaseEntry myrecord = new DatabaseEntry();
				new RecordTupleBinding().objectToEntry(mydata, myrecord);

				myDatabase.put(null, myKey, myrecord);

				i += 1;
				System.out.println("insert record " + i + " successfully!");
	

			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

		}
		
		Iterator<Entry<String, SecondaryDatabase>> iterator = openSecdbMap.entrySet().iterator();
		for(Entry e : opendbMap.entrySet())
		{
			Database secDB = iterator.next().getValue();
			Database primaryDB = (Database)e.getValue();
			secDB.close();
		    primaryDB.close();
		    
		}

		if (myDBenvironment != null) {
			myDBenvironment.close();
		}
		
		
		///// The Parameters
		///// The Parameters
		///// The Parameters
		System.out.println("The end of insertion for " + fileName + " file!!!!");

	}

}
