package Dim3;


import java.io.File;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;

public class NaiveSkyline {

	static final String filepath = "./data/normal-5m";

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		String[] skyline = { "AgeIs", "IncomeIs", "HeightIs"};
		List<Comparison> comparisons = new ArrayList<Comparison>();
		// comparisons.add(Comparison.MIN);
		// comparisons.add(Comparison.MIN);
		// comparisons.add(Comparison.MIN);
		 comparisons.add(Comparison.MIN);
		comparisons.add(Comparison.MIN);
		comparisons.add(Comparison.MIN);

		ArrayList<Tuple> windowTuples = new ArrayList<Tuple>();

		long startTime = System.currentTimeMillis();
		ArrayList<Tuple> tuplesList = RDFJoin(skyline, comparisons, null);
		
		Tuple[] skylineTuples = BNL.CalculateSkyline(tuplesList, null, comparisons);
		long endTime = System.currentTimeMillis();
		System.out.println("skyline tuples: " + Arrays.asList(skylineTuples));
		int l = Arrays.asList(skylineTuples).size();
		System.out.println("The length of skyline result is: " + l);
		System.out.println("New Naive BNL Used Time is: " + ((endTime - startTime) / 1000.0) + "(s)");

	}

	private static ArrayList<Tuple> RDFJoin(String[] skylineAttr,
			List<Comparison> comparisons, List<Tuple> windowTuples)
			throws SQLException {
		// TODO Auto-generated method stub
		
		// create environment
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);

		// Environment myDbEnvironment = new Environment(new File("."),
		// envConfig);
		Environment myDbEnvironment = new Environment(new File(filepath),
				envConfig);

		// create database
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(true);

		Database mydb = null;

		// create secondary database
		SecondaryConfig secConfig = new SecondaryConfig();
		secConfig.setAllowCreate(true);
		secConfig.setSortedDuplicates(true);
		secConfig.setKeyCreator(new SecondKeyCreator());

		SecondaryDatabase secDB = null;

		ArrayList<Database> dbList = new ArrayList<Database>();
		ArrayList<SecondaryDatabase> secDbList = new ArrayList<SecondaryDatabase>();
		ArrayList<SecondaryCursor> secCursorList = new ArrayList<SecondaryCursor>();

		// open all the berkeleyDB
		for (int i = 0; i < skylineAttr.length; i++) {
			String dbname = skylineAttr[i] + "db";
			String secDbname = skylineAttr[i] + "SecDB";
			mydb = myDbEnvironment.openDatabase(null, dbname, dbConfig);
			secDB = myDbEnvironment.openSecondaryDatabase(null, secDbname,
					mydb, secConfig);

			// add dbs to dblist
			dbList.add(mydb);
		}

		// read data
		Cursor cursor = dbList.get(0).openCursor(null, null);

		DatabaseEntry readKey = new DatabaseEntry();
		DatabaseEntry readData = new DatabaseEntry();
		

		ArrayList<Tuple> tuplesList = new ArrayList<Tuple>();
		
		while (cursor.getNext(readKey, readData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			ArrayList<Object> temp = new ArrayList<Object>();
			
			Record record = (Record) new RecordTupleBinding()
					.entryToObject(readData);
			try {
				String key = new String(readKey.getData(), "UTF-8");
				
				int objVal = record.getobjectVal();

				temp.add(objVal);

				for (int j = 1; j < skylineAttr.length; j++) {
//					System.out.println("j is " + j);
					DatabaseEntry Data = new DatabaseEntry();
					if (dbList.get(j)
							.get(null, readKey, Data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
//						System.out.println("inside");
						Record record2 = (Record) new RecordTupleBinding()
								.entryToObject(Data);
						int objVal2 = record2.getobjectVal();
//						System.out.println("read objval " + j +" is " + objVal2);
//						tuple[j] = objVal2;
						temp.add(objVal2);
					}

				}
				// System.out.println("the record " + key + " is: objVal-" +
				// objVal);

			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			Tuple tuple = new Tuple(temp);
			tuplesList.add(tuple);	
		}

//		System.out.println("tupleList is: " + tuplesList.toString());
		cursor.close();

		return tuplesList;
	}

	
	private static void printCandidateList(ArrayList<Tuple> candidateList) {
		System.out.println("candidate list is: ");

		for (int i = 0; i < candidateList.size(); i++) {
			Tuple currenttuple = candidateList.get(i);

			System.out.println("tuple: " + currenttuple);

		}
	}

}
