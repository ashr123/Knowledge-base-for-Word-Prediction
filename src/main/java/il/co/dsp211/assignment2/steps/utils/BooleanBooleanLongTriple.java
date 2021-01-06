package il.co.dsp211.assignment2.steps.utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class BooleanBooleanLongTriple implements WritableComparable<BooleanBooleanLongTriple>
{
	private boolean isTriGram, isGroup0;
	private long r;

	public BooleanBooleanLongTriple()
	{
	}

	public BooleanBooleanLongTriple(boolean isTriGram, boolean isGroup0, long r)
	{
		this.isTriGram = isTriGram;
		this.isGroup0 = isGroup0;
		this.r = r;
	}

	public static BooleanBooleanLongTriple of(String string)
	{
		final String[] values = string.split("🤠");
		return new BooleanBooleanLongTriple(Boolean.parseBoolean(values[0]), Boolean.parseBoolean(values[1]), Long.parseLong(values[2]));
	}

	public boolean isTriGram()
	{
		return isTriGram;
	}

	public boolean isGroup0()
	{
		return isGroup0;
	}

	public long getR()
	{
		return r;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (!(o instanceof BooleanBooleanLongTriple))
			return false;
		BooleanBooleanLongTriple that = (BooleanBooleanLongTriple) o;
		return isTriGram == that.isTriGram &&
		       isGroup0 == that.isGroup0 &&
		       r == that.r;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(isTriGram, isGroup0, r);
	}

	/**
	 * for record ordering: value is [⟨T<sub>r</sub>, N<sub>r</sub>⟩] with 1 pair for each r and group. Suppose to happen before a record with TriGrams
	 *
	 * @param o the object to be compared.
	 * @return negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object.
	 */
	@Override
	public int compareTo(BooleanBooleanLongTriple o)
	{
		final int
				compareR = Long.compare(r, o.r),
				compareIsGroup0;

		return compareR != 0 ? compareR :
		       (compareIsGroup0 = Boolean.compare(isGroup0, o.isGroup0)) != 0 ? compareIsGroup0 :
		       Boolean.compare(isTriGram, o.isTriGram);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeBoolean(isTriGram);
		out.writeBoolean(isGroup0);
		out.writeLong(r);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		isTriGram = in.readBoolean();
		isGroup0 = in.readBoolean();
		r = in.readLong();
	}

	@Override
	public String toString()
	{
		return isTriGram + "🤠" + isGroup0 + "🤠" + r;
	}
}
