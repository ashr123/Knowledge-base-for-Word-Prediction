package il.co.dsp211.utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class BooleanLongPair implements Writable
{
	private boolean key;
	private long value;

	public BooleanLongPair()
	{
	}

	public BooleanLongPair(boolean key, long value)
	{
		this.key = key;
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeBoolean(key);
		out.writeLong(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		key = in.readBoolean();
		value = in.readLong();
	}

	public boolean isKey()
	{
		return key;
	}

	public long getValue()
	{
		return value;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (!(o instanceof BooleanLongPair))
			return false;
		BooleanLongPair that = (BooleanLongPair) o;
		return key == that.key && value == that.value;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(key, value);
	}

	@Override
	public String toString()
	{
		return "BooleanLongPair{" +
		       "key=" + key +
		       ", value=" + value +
		       '}';
	}
}
