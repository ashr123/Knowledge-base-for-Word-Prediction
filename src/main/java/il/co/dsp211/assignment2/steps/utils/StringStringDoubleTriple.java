package il.co.dsp211.assignment2.steps.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StringStringDoubleTriple implements WritableComparable<StringStringDoubleTriple>
{
	private String string1, string2;
	private double prob;

	public StringStringDoubleTriple()
	{
	}

	public StringStringDoubleTriple(String string1, String string2, double prob)
	{
		this.string1 = string1;
		this.string2 = string2;
		this.prob = prob;
	}

	public String getString1()
	{
		return string1;
	}

	public String getString2()
	{
		return string2;
	}

	public double getProb()
	{
		return prob;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (!(o instanceof StringStringDoubleTriple))
			return false;
		StringStringDoubleTriple that = (StringStringDoubleTriple) o;
		return Double.compare(that.prob, prob) == 0 &&
		       string1.equals(that.string1) &&
		       string2.equals(that.string2);
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(string1, string2, prob);
	}

	@Override
	public int compareTo(StringStringDoubleTriple o)
	{
		final int
				string1Compare = string1.compareTo(o.string1),
				string2Compare = string2.compareTo(o.string2);

		return string1Compare != 0 ? string1Compare :
		       string2Compare != 0 ? string2Compare :
		       Double.compare(o.prob, prob);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		Text.writeString(out, string1);
		Text.writeString(out, string2);
		out.writeDouble(prob);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		string1 = Text.readString(in);
		string2 = Text.readString(in);
		prob = in.readDouble();
	}

	@Override
	public String toString()
	{
		return string1 + "🤠" + string2 + "🤠" + prob;
	}
}
