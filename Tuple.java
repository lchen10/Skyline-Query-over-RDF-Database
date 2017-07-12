package Dim3;




import java.util.ArrayList;
import java.util.List;

public class Tuple {
	private static boolean isDebug = false;
	private List<Integer> values = new ArrayList<Integer>();

	private static void debug(String text) {
		// TODO Auto-generated method stub
		if(isDebug){
			System.out.println(text);
		}
	}
	
	public Tuple(int[] data) {
		// TODO Auto-generated constructor stub
		values.clear();
		for(int i : data){
			values.add(i);
		}
	}

	public Tuple() {
		// TODO Auto-generated constructor stub
	}

	public Tuple(ArrayList<Object> data) {
		values.clear();
		for(Object i : data){
			values.add((Integer) i);
		}
	}

	public Tuple(Object[] data) {
		values.clear();
		for(Object i : data){
			values.add((Integer) i);
		}
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
        StringBuffer sb = new StringBuffer("<");
        for(int i : values){
        	sb.append(i +", ");
        }
        sb.append(">\n");
        return sb.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((values == null) ? 0 : values.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Tuple other = (Tuple) obj;
		if (values == null) {
			if (other.values != null)
				return false;
		} else {
			if(other == null){
				return false;
			}
			if(values.size() != other.values.size()){
				return false;
			}
			for(int i = 0;i< values.size();i++){
				if(!values.get(i).equals(other.values.get(i))){
					return false;					
				}
			}
		}
		return true;
	}

	public int size() {
		// TODO Auto-generated method stub
		return values.size();
	}

	public int dominate(Tuple otherTuple, List<Comparison> comparisons) {
		return dominate(otherTuple,(Comparison[])comparisons.toArray());
	}
	
	public int dominate(Tuple otherTuple, Comparison[] comparisons) {
		
		if(values.size() != otherTuple.size()){
			return -1;
		}
		
		boolean strickRelation = false;
		int equalTimes = 0;
		for(int i = 0;i < values.size();i++){
			Integer self = values.get(i);
			Integer other = otherTuple.values.get(i);
			debug("self: " + self);
			debug("other: " + other);
			Comparison comparison = null;
			if(comparisons == null){
				comparison = Comparison.MAX;
			}
			else{
				if(i < comparisons.length ){
					comparison = comparisons[i];
				}else{
				comparison = Comparison.MAX;
				}
			}
			
			debug("comparison: " + comparison);
			
			if(self.equals(other)){
				
				equalTimes++;
				debug("self == other");
				debug("equalTimes: " + equalTimes);
				debug("values: " + values.size());
				if(equalTimes == values.size()){
					debug("tuples equal! ");
					return 0;
				}
				continue;
			}
			
			if(comparison == Comparison.JOIN){
				continue;
			}
			
			if(comparison == Comparison.MAX){
				debug("inside max ");
				if(self > other){
					strickRelation = true;
					continue;
				}
				debug("begin return -1 ");
				return -1;
			}
			if(comparison == Comparison.MIN){
				if(self < other)
				{
					strickRelation = true;
					continue;
				}
				return -1;
			}			
		}
		if(strickRelation){
			return 1;
		}
		
		return -1;
	}

	public int getValue(int index) {
		// TODO Auto-generated method stub
		return values.get(index);
	}

	public List<Integer> getValues() {
		// TODO Auto-generated method stub
		return values;
	}

	public void addValues(List<Integer> list) {
		// TODO Auto-generated method stub
		values.addAll(list);
		
	}

	public void add(int value) {
		// TODO Auto-generated method stub
		values.add(value);
	}

	
}
