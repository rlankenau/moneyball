package com.mapr.baseball;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import java.util.regex.*;

/* Output from exec is 5-tuple: (bases, fielder, field location, type of hit, RBIs)  */
public class EventParser extends EvalFunc<Tuple>
{
	TupleFactory mTupleFactory =  TupleFactory.getInstance();
	Pattern event_pattern = Pattern.compile("([SDT]|HR)?([1-9]+)(?:/(B?[GLPF]+)?([1-9]+M?[XL]?[DS]F?))?(?:\\.(.*))?");


	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() != 1) 
			return null;
		try {
			String inputstr;
			Object o = input.get(0).toString();
			if (o instanceof String)
			{
				inputstr = (String)o;
			}
			else
			{
				throw new IOException("Unexpected type supplied.  Expected String, got " + o.getClass().getName() );
			}
			Matcher m = event_pattern.matcher(inputstr);
			if(m.matches() != true) {
				Tuple t = mTupleFactory.newTuple(2);
				t.set(0, "Error");
				t.set(1, "Input row didn't match the expected pattern: " + inputstr);
				return t;
			} 
			/* Create a new Tuple to hold the result */
			Tuple t = mTupleFactory.newTuple(5);
			for(int i=0;i<5;i++)
			{
				t.set(i, m.group(i+1));
			}
			return t;
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row." + e);
		}
	} 
} 
