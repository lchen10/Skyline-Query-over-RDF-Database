package Dim3;



import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class RecordTupleBinding extends TupleBinding{

	@Override
	public Object entryToObject(TupleInput ti) {
		// TODO Auto-generated method stub
		Record record = new Record();
		record.setobjectVal(ti.readInt());

		return record;
	}

	@Override
	public void objectToEntry(Object obj, TupleOutput to) {
		// TODO Auto-generated method stub
		Record record = (Record) obj;
		to.writeInt(record.getobjectVal());
	}
	
	

}
