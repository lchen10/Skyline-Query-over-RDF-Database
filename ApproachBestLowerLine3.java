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

public class ApproachBestLowerLine3 {

	// This algorithm is for the algorithm of RDFSkyJoinWithPartialHeader+(RSJPH+)
	static final String filepath = "./data/normal-5m";

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		String[] skyline = { "AgeIs", "IncomeIs", "HeightIs" };
		List<Comparison> comparisons = new ArrayList<Comparison>();
		// comparisons.add(Comparison.MIN);
		// comparisons.add(Comparison.MIN);
		// comparisons.add(Comparison.MIN);
		comparisons.add(Comparison.MIN);
		comparisons.add(Comparison.MIN);
		comparisons.add(Comparison.MIN);

		ArrayList<Tuple> windowTuples = new ArrayList<Tuple>();

		long startTime = System.currentTimeMillis();

		try {
			ArrayList<Tuple> candidateList = ApproachBestLowerLine3.RDFJoin(
					skyline, comparisons, windowTuples);
			//candidateList.addAll(windowTuples);
			windowTuples.addAll(candidateList);
			System.out.println("before BNL, number of tuples: "
					+ windowTuples.size());
			
			Tuple[] skylineTuples = BNL.CalculateSkyline(windowTuples, null,
					comparisons);
			long endTime = System.currentTimeMillis();
			
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

        // calculate the running time for RSJPH+
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

		// join first tuples from each VPT
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
		int foundBetterTuples = 0;

		boolean partialUpdate = false;
		boolean[] goodDims = new boolean[skylineAttr.length];

		Tuple[] currentTuples = new Tuple[skylineAttr.length];

		// while loop is used for comparing each newly joined tuple with the header point and partially update the header point
		// the difference between RSJPH and RSJPH+ is that we add a crosscheck before we prune a newly joined tuple

		while (!stop) {
			// System.out.println("Inside big while stop");
			// System.out.println("+++++++++++++++++++++++++++++++++++++");

			int[] colBetterTimes = new int[skylineAttr.length];
			boolean needToStop = true;
			int dimStop = 0;

			for (int i = 0; i < skylineAttr.length; i++) {

				if (stop) {
					// no need to check other column
					break;
				}

				// find a tuple in certain dimension which is better than header
				// point	

				Object[] currenttuple = new Object[skylineAttr.length];

				// System.out.println("tupleIndex is: " + tupleIndexes[i]);
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
							// int otherObj = dataMaps.get(j).get(id);
							// currenttuple[j] = otherObj;
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
				int lastBetterColIndex = -1;

				for (int headIndex = 0; headIndex < head.length; headIndex++) {
					if (comparisons.get(headIndex) == Comparison.MAX) {
						if (((Integer) currenttuple[headIndex]) > head[headIndex]) {
							betterTimes++;
							colBetterTimes[headIndex]++;
							// needToStop = false;
							lastBetterColIndex = headIndex;
						}
					} else {
						if (((Integer) currenttuple[headIndex]) < head[headIndex]) {
							betterTimes++;
							colBetterTimes[headIndex]++;
							// needToStop = false;
							lastBetterColIndex = headIndex;
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

				} 
				// If the newly joined tuple has some dimensional values better than "non-updated"
				// dimension and some dimensional values worse than "updated" dimension, we add
				// this tuple into candidate list.
				else if (betterTimes == 1 && partialUpdate) {
					if (!goodDims[lastBetterColIndex]) {
						Tuple tuple = new Tuple(currenttuple);
						if (!candidateMap.containsKey(key)) {
							candidateList.add(tuple);
						}
					}
				}

				else {
					// System.out.println("tuple " + id + " is pruned!");
				}
				currentTuples[i] = new Tuple(currenttuple);
				candidateMap.put(key, currenttuple);

			}
			boolean isUpdate = false;
			if (!stop) {
				// partially update the header point: for the ith deimension, the value is
				// updated only if the worst value of n newly joined tuples in the ith 
				// dimension is better than the ith dimensional value in H.
				int updateColCount = 0;
				for (int index = 0; index < skylineAttr.length; index++) {
					if (colBetterTimes[index] == skylineAttr.length) {
						isUpdate = true;
						updateColCount++;
						int worst = (Integer) currentTuples[0].getValue(index); // use
						goodDims[index] = true; // good column

						for (int j = 1; j < currentTuples.length; j++) {
							Tuple currentTuple = currentTuples[j];
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
					} else {
						goodDims[index] = false;
					}
					// bitCount[index] = 0;
				}

				if (updateColCount < skylineAttr.length) {
					partialUpdate = true;
				} else {
					partialUpdate = false;
				}

				// terminating condition: the header point has no update and all the dimensional
				// values in the newly joined tuples are worse than that of current header point.
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

		System.out.println("windowTuples: " + windowTuples);
		printCandidateList(candidateList);

		return candidateList;

		// ArrayList<Tuple> tuplesList = new ArrayList<Tuple>();
		// return tuplesList;
	}

	private static void printCandidateList(ArrayList<Tuple> candidateList) {
		System.out.println("candidate list is: ");
		try {
			// Create file
			FileWriter fstream = new FileWriter("candidalistLowerLine.txt");
			BufferedWriter out = new BufferedWriter(fstream);
			for (int i = 0; i < candidateList.size(); i++) {
				Tuple currenttuple = candidateList.get(i);
				out.write("tuple: " + currenttuple);

			}
			out.close();
		} catch (Exception e) {// Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}

	}

}
