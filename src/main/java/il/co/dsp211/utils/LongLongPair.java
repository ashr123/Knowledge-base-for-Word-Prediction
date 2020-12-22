package il.co.dsp211.utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class LongLongPair implements Writable
{
	private long key, value;

	public LongLongPair()
	{
	}

	public LongLongPair(long key, long value)
	{
		this.key = key;
		this.value = value;
	}

	public long getKey()
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
		if (!(o instanceof LongLongPair))
			return false;
		LongLongPair that = (LongLongPair) o;
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
		return "LongLongPair{" +
		       "key=" + key +
		       ", value=" + value +
		       '}';
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(key);
		out.writeLong(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		key = in.readLong();
		value = in.readLong();
	}
}
