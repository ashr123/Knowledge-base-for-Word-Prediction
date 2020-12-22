package il.co.dsp211.utils;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ObjectPair<K, V> implements Writable
{
	private K key;
	private V value;

	public ObjectPair()
	{
	}

	public ObjectPair(K key, V value)
	{
		this.key = key;
		this.value = value;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		ObjectWritable.writeObject(out, key, key.getClass(), null);
		ObjectWritable.writeObject(out, value, value.getClass(), null);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		key = (K) ObjectWritable.readObject(in, null);
		value = (V) ObjectWritable.readObject(in, null);
	}

	public K isKey()
	{
		return key;
	}

	public V getValue()
	{
		return value;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (!(o instanceof ObjectPair))
			return false;
		ObjectPair<?, ?> that = (ObjectPair<?, ?>) o;
		return Objects.equals(key, that.key) &&
		       Objects.equals(value, that.value);
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
