package simpledb.storage;

import simpledb.common.Type;
import simpledb.execution.Predicate;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Instance of Field that stores a single String of a fixed length.
 */
public class StringField implements Field {

	private static final long serialVersionUID = 1L;

	private final String value;
	private final int maxSize;

	public String getValue() {
		return this.value;
	}

	/**
	 * Constructor.
	 * 
	 * @param s
	 *            The value of this field.
	 * @param maxSize
	 *            The maximum size of this string
	 */
	public StringField(String s, int maxSize) {
		this.maxSize = maxSize;

		if (s.length() > maxSize) {
			this.value = s.substring(0, maxSize);
		} else {
			this.value = s;
		}
	}

	@Override
	public String toString() {
		return this.value;
	}

	@Override
	public int hashCode() {
		return this.value.hashCode();
	}

	@Override
	public boolean equals(Object field) {
	    if (!(field instanceof StringField)) {
			return false;
		}
		return ((StringField) field).value.equals(this.value);
	}

	/**
	 * Write this string to dos. Always writes maxSize + 4 bytes to the passed
	 * in dos. First four bytes are string length, next bytes are string, with
	 * remainder padded with 0 to maxSize.
	 * 
	 * @param dos
	 *            Where the string is written
	 */
	@Override
	public void serialize(DataOutputStream dos) throws IOException {
		String s = this.value;
		int overflow = this.maxSize - s.length();
		if (overflow < 0) {
            s = s.substring(0, this.maxSize);
		}
		dos.writeInt(s.length());
		dos.writeBytes(s);
		while (overflow-- > 0) {
			dos.write((byte) 0);
		}
	}

	/**
	 * Compare the specified field to the value of this Field. Return semantics
	 * are as specified by Field.compare
	 * 
	 *             if val is not a StringField
	 * @see Field#compare
	 */
	@Override
	public boolean compare(Predicate.Op op, Field val) {

		StringField iVal = (StringField) val;
		int cmpVal = this.value.compareTo(iVal.value);

		switch (op) {
		case EQUALS:
			return cmpVal == 0;

		case NOT_EQUALS:
			return cmpVal != 0;

		case GREATER_THAN:
			return cmpVal > 0;

		case GREATER_THAN_OR_EQ:
			return cmpVal >= 0;

		case LESS_THAN:
			return cmpVal < 0;

		case LESS_THAN_OR_EQ:
			return cmpVal <= 0;

		case LIKE:
			return this.value.contains(iVal.value);
		}

		return false;
	}

	/**
	 * @return the Type for this Field
	 */
	@Override
	public Type getType() {

		return Type.STRING_TYPE;
	}
}
