package Dim3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class ApproachBestLowerLine2Normal {

	// This algorithm is for the algorithm of RDFSkyJoinWithFullHeader(RSJFH)

	static final String filepath = "./sampledata";

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		// String[] skyline = { "landArea", "popDensity"};
		String[] skyline = { "AgeIs", "IncomeIs" };
		List<Comparison> comparisons = new ArrayList<Comparison>();
		// comparisons.add(Comparison.MIN);
		// comparisons.add(Comparison.MIN);
		// comparisons.add(Comparison.MIN);
		//comparisons.add(Comparison.MIN);
		comparisons.add(Comparison.MIN);
		comparisons.add(Comparison.MIN);

		ArrayList<Tuple> windowTuples = new ArrayList<Tuple>();

		long startTime = System.currentTimeMillis();

		try {
			ArrayList<Tuple> candidateList = ApproachBestLowerLine2Normal
					.RDFJoin(skyline, comparisons, windowTuples);
			windowTuples.addAll(candidateList);

			System.out.println("before BNL, number of tuples: "
					+ windowTuples.size());
			Tuple[] skylineTuples = BNL.CalculateSkyline(windowTuples, null,
					comparisons);
			long endTime = System.currentTimeMillis();

			 System.out.println("skyline tuples: " + Arrays.asList(skylineTuples));
			int l = Arrays.asList(skylineTuples).size();
			System.out.println("The length of skyline result is: " + l);
			System.out.println("Approaching Best Used Time is: "
					+ ((endTime - startTime) / 1000.0) + "(s)");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static ArrayList<Tuple> RDFJoin(String[] skylineAttr,
			List<Comparison> comparisons, List<Tuple> windowTuples)
			throws SQLException {
		// TODO Auto-generated method stub

		// calculate the running time of RSJFH
		long startTime = System.currentTimeMillis();

		long endTime = System.currentTimeMillis();
		System.out.println("after reading data from berkerly db is: "
				+ ((endTime - startTime) / 1000.0) + "(s)");
		// System.out.println("data: " + data);

		// create environment
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);

		Environment myDbEnvironment = new Environment(new File(filepath),
				envConfig);

		// create primary database
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

		// save dbs, sec dbs, cursor
		for (int i = 0; i < skylineAttr.length; i++) {

			String dbname = skylineAttr[i] + "db";
			String secDbname = skylineAttr[i] + "SecDB";
			mydb = myDbEnvironment.openDatabase(null, dbname, dbConfig);
			secDB = myDbEnvironment.openSecondaryDatabase(null, secDbname,
					mydb, secConfig);

			// read primary db
			Cursor cursor = mydb.openCursor(null, null);

			// read sec db
			SecondaryCursor cursor2 = secDB.openSecondaryCursor(null, null);

			dbList.add(mydb);
			secDbList.add(secDB);
			secCursorList.add(cursor2);
		}

		
		ArrayList<Tuple> candidateList = new ArrayList<Tuple>();
		HashMap<String, Object[]> candidateMap = new HashMap<String, Object[]>();
		boolean match = false;

		// join first tuples in each VPT
		ArrayList<Object[]> firstTuples = new ArrayList<Object[]>();

		for (int i = 0; i < skylineAttr.length; i++) {
			SecondaryCursor secCursor = secCursorList.get(i);

			DatabaseEntry PrimKey = new DatabaseEntry();
			DatabaseEntry SecreadKey = new DatabaseEntry();
			DatabaseEntry SecreadData = new DatabaseEntry();

			Object[] tuple = new Object[skylineAttr.length];
			String key = null;

	    	// get the record from each VPT in BerkeleyDB
			if (secCursor.getNext(SecreadKey, PrimKey, SecreadData,
					LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				Record record = (Record) new RecordTupleBinding()
						.entryToObject(SecreadData);

				try {
					key = new String(PrimKey.getData(), "UTF-8");
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				int objVal = record.getobjectVal();
				tuple[i] = objVal;
			} else {
				// System.out.println("Reading data is finished!!!!");
			}


			// hash join to get first complet tuples
			for (int j = 0; j < skylineAttr.length; j++) {
				if (j == i) {
					continue;
				}
				DatabaseEntry Data = new DatabaseEntry();
				if (dbList.get(j).get(null, PrimKey, Data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
					Record record = (Record) new RecordTupleBinding()
							.entryToObject(Data);
					int objVal = record.getobjectVal();
					tuple[j] = objVal;
				} else {
					System.out
							.println("did not get the value using primary key!!!");
				}
			}

			Tuple newTuple = new Tuple(tuple);

			if (!candidateMap.containsKey(key)) {
				windowTuples.add(newTuple);
				candidateMap.put(key, tuple);
			}
		}

		if (match) {
			// System.out.println("found match for the first two tuples");
			// System.out.println("window tuple: " + windowTuples);
			// printCandidateList(candidateList);
			// /// return list;
		}

		// System.out.println("first foundmap is: " + foundTupleTimes);


		// compute the first head point
		Integer[] head = new Integer[skylineAttr.length];
		for (int i = 0; i < skylineAttr.length; i++) {
			int worst = (Integer) windowTuples.get(0).getValue(i);
			for (int j = 1; j < windowTuples.size(); j++) {
				Tuple currenttuple = (Tuple) windowTuples.get(j);
				if (comparisons.get(i) == Comparison.MAX) {
					if (((Integer) currenttuple.getValue(i)) < worst) {
						worst = ((Integer) currenttuple.getValue(i));
						// System.out.println("MAX worst is:" + worst);
					}
				} else {
					if (((Integer) currenttuple.getValue(i)) > worst) {
						worst = ((Integer) currenttuple.getValue(i));
						// System.out.println("MIN worst is:" + worst);
					}
				}
			}
			head[i] = worst;
		}

		System.out.print("found first head: ");
		for (int l = 0; l < head.length; l++) {
			System.out.print(head[l] + " ");
		}
		System.out.println();

		boolean stop = false;
		int foundBetterDim = 0;

		Tuple[] currentTuples = new Tuple[skylineAttr.length];
		Tuple[] betterTuples = new Tuple[skylineAttr.length];
		int prunecount = 0;

		// while loop is used for finding a pair of 3-dim Header Tuples which
		// contribute to updating a new Header Point
		while (!stop) {
			// System.out.println("Inside big while stop");
			// System.out.println("+++++++++++++++++++++++++++++++++++++");

			boolean needToStop = true;
			int dimStop = 0;

			// need to find the Header Tuple in each dimension
			for (int i = 0; i < skylineAttr.length; i++) {

				if (stop) {
					// no need to check other column
					break;
				}


				// find a tuple in certain dimension which is better than header point				
				Object[] currenttuple = new Object[skylineAttr.length];
				
				SecondaryCursor secCursor = secCursorList.get(i);

				DatabaseEntry PrimKey = new DatabaseEntry();
				DatabaseEntry SecreadKey = new DatabaseEntry();
				DatabaseEntry SecreadData = new DatabaseEntry();

				String key = null;

				if (secCursor.getNext(SecreadKey, PrimKey, SecreadData,
						LockMode.DEFAULT) == OperationStatus.SUCCESS) {
					Record record = (Record) new RecordTupleBinding()
							.entryToObject(SecreadData);

					try {
						key = new String(PrimKey.getData(), "UTF-8");

					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
					}

					if (candidateMap.containsKey(key)) {
						currenttuple = candidateMap.get(key);
					} else {
						int objVal = record.getobjectVal();
						currenttuple[i] = objVal;
						for (int j = 0; j < skylineAttr.length; j++) {
							if (j == i) {
								continue; // do not join self
							}
							DatabaseEntry Data = new DatabaseEntry();
							if (dbList.get(j).get(null, PrimKey, Data,
									LockMode.DEFAULT) == OperationStatus.SUCCESS) {
								Record record2 = (Record) new RecordTupleBinding()
										.entryToObject(Data);
								int objVal2 = record2.getobjectVal();
								currenttuple[j] = objVal2;
							} else {
								System.out
										.println("did not get the value using primary key for "
												+ skylineAttr[i] + "  !!!!");
							}
						}
					}

				} else {
					System.out.println("Reading data is finished for "
							+ skylineAttr[i] + "  !!!!");
					stop = true;
					System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
					System.out.println("scan all tuples in column "
							+ skylineAttr[i]);
					System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
					break;
				}

				int betterTimes = 0;

				// to calculate the number of dimensions at which a newly joined tuple is better than the header point 
				for (int headIndex = 0; headIndex < head.length; headIndex++) {
					if (comparisons.get(headIndex) == Comparison.MAX) {
						if (((Integer) currenttuple[headIndex]) > head[headIndex]) {
							betterTimes++;

						}
					} else {
						if (((Integer) currenttuple[headIndex]) < head[headIndex]) {
							betterTimes++;

						}
					}
				}

				if (comparisons.get(i) == Comparison.MAX) {
					if (((Integer) currenttuple[i]) <= head[i]) {
						dimStop++;
					}

				} else {
					if (((Integer) currenttuple[i]) >= head[i]) {
						dimStop++;
					}
				}

				if (betterTimes > 1) {
					Tuple tuple = new Tuple(currenttuple);
					// System.out.println("adding key: " + key + "-> " + tuple
					// + "  into candidate list");
					if (!candidateMap.containsKey(key)) {
						candidateList.add(tuple);
					}
					if (betterTuples[i] == null) {
						foundBetterDim++;
					}
					betterTuples[i] = new Tuple(currenttuple);

				} else {
					// System.out.println("tuple " + id + " is pruned!");
					prunecount++;

				}
				currentTuples[i] = new Tuple(currenttuple);
				candidateMap.put(key, currenttuple);

			}

			boolean isUpdate = false;
			if (!stop) {
				// update the header point
				if (foundBetterDim == skylineAttr.length) {
					isUpdate = true;
					for (int index = 0; index < skylineAttr.length; index++) {
						int worst = (Integer) betterTuples[0].getValue(index); // use
						for (int j = 1; j < betterTuples.length; j++) {
							Tuple currentTuple = betterTuples[j];
							if (comparisons.get(index) == Comparison.MAX) {
								if (((Integer) currentTuple.getValue(index)) < worst) {
									worst = ((Integer) currentTuple
											.getValue(index));
								}
							} else {
								if (((Integer) currentTuple.getValue(index)) > worst) {
									worst = ((Integer) currentTuple
											.getValue(index));
								}
							}

						}
						head[index] = worst;
					}

					foundBetterDim = 0;
					betterTuples = new Tuple[skylineAttr.length];

				}

				// currentTuples = new
				// the terminating condition: If the header point is not updated anymore and the 
				if (!isUpdate && dimStop == skylineAttr.length) {
					System.out.println("stop algorithm!!");
					for (int j = 0; j < currentTuples.length; j++) {
						System.out.println("stopping tuple: "
								+ currentTuples[j]);

					}
					System.out.println("head point: " + new Tuple(head));
					stop = true;
					break;
				}

			}
		}

		// System.out.println("windowTuples: " + windowTuples);
		// printCandidateList(candidateList);

		System.out.println("prune count is: " + prunecount);
		return candidateList;

		// ArrayList<Tuple> tuplesList = new ArrayList<Tuple>();
		// return tuplesList;
	}


}
