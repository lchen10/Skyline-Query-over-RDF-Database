package Dim3;




import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BNL {

	public static boolean isDebug = false;
	public static boolean writeToFile = false;

	public static Tuple[] CalculateSkyline(Tuple[] tuples) {
		// TODO Auto-generated method stub
		return CalculateSkyline(tuples, null);
	}

	public static Tuple[] removeFirst(Tuple[] array){
		if(array.length <= 1){
			return new Tuple[0];
		}
		
		Tuple[] result = new Tuple[array.length - 1];
		for(int i = 1;i<array.length;i++){
			result[i - 1] = array[i];
		}
		return result;
	}
	public static Tuple[] CalculateSkyline(Tuple[] tuples,
			Comparison[] comparisons) {

		Tuple[] tuplesToBeComputed = removeFirst(tuples);
		Tuple[] initialSkylineTuples = new Tuple[] { tuples[0] };
		return CalculateSkyline(tuplesToBeComputed, initialSkylineTuples,
				comparisons);
	}

	private static void debug(String text) {
		if (isDebug) {
			System.out.println(text);
		}
		if (writeToFile) {
			BufferedWriter bw = null;

			try {
				bw = new BufferedWriter(new FileWriter("temp", true));
				bw.write(text);
				bw.flush();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			} finally { // always close the file
				if (bw != null)
					try {
						bw.close();
					} catch (IOException ioe2) {
						// just ignore it
					}
			}
		}
	}

	public static Tuple[] CalculateSkyline(Tuple[] tuplesToBeComputed,
			Tuple[] initialSkylineTuples, Comparison[] comparisons) {
		// TODO Auto-generated method stub
		debug("============");
		debug("Start calculate skyline");
		List<Tuple> result = new ArrayList<Tuple>();
		debug("Add initialSkylineTuples...");
		for (Tuple t : initialSkylineTuples) {
			debug("add " + t);
			result.add(t);
		}

		for (int i = 0; i < tuplesToBeComputed.length; i++) {
			Tuple tobeChecked = tuplesToBeComputed[i];
			boolean needToAdd = true;
			for (int j = 0; j < result.size(); j++) {
				Tuple current = result.get(j);
				debug("current: " + current);
				debug("tobeChecked: " + tobeChecked);
				int dominanceValue = current.dominate(tobeChecked, comparisons);
				debug("dominanceValue: " + dominanceValue);
				if (dominanceValue == 1 || dominanceValue == 0) {
					debug("check next tuple");
					needToAdd = false;
					break;
				}
				if (tobeChecked.dominate(current, comparisons) == 1) {
					debug(tobeChecked + " dominate " + current);
					debug("removing " + current);
					result.remove(j);
					j--;
				}
			}
			if (needToAdd) {
				debug("Add " + tobeChecked);
				result.add(tobeChecked);
			}
		}
		debug("End");
		return (Tuple[]) result.toArray(new Tuple[result.size()]);
	}

	public static Tuple[] CalculateSkyline(List<Tuple> group,
			Comparison[] comparisons) {
		// TODO Auto-generated method stub
		return CalculateSkyline(group.toArray(new Tuple[group.size()]),
				comparisons);
	}

	public static Tuple[] CalculateSkyline(List<Tuple> group,
			List<Comparison> comparisons) {
		// TODO Auto-generated method stub
		return CalculateSkyline(group.toArray(new Tuple[group.size()]),
				comparisons.toArray(new Comparison[comparisons.size()]));
	}

	public static Tuple[] CalculateSkyline(List<Tuple> tuplesTobeComputed,
			List<Tuple> partialSkyline, List<Comparison> comparisons) {
		Comparison[] comparisonArray = comparisons
				.toArray(new Comparison[comparisons.size()]);
		Tuple[] tuplesTobeComputedArray = tuplesTobeComputed
				.toArray(new Tuple[tuplesTobeComputed.size()]);
		if (partialSkyline == null) {
			return CalculateSkyline(tuplesTobeComputedArray, new Tuple[0],
					comparisonArray);
		}
		return CalculateSkyline(tuplesTobeComputedArray, partialSkyline
				.toArray(new Tuple[partialSkyline.size()]), comparisonArray);

	}
}
