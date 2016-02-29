package systems.speedy.distributed.dictionary;

/** A distributed dictionary supporting up to 256 unique Strings.
 */
public interface DistributedByteDictionary {
	/**
	 * @param s The string to lookup a unique byte code for.
	 * @return The byte representing the String s. If a unique byte cannot be returned for s then a Runtime Exception is thrown (this is a coding error).
	 */
	public byte lookup(String s);

	/**
	 * @param code The byte code for a String.
	 * @return The String for the byte code. If a String is not stored for the byte code then a Runtime Exception is thrown (this is a coding error).
	 */
	public String lookup(byte code);
}
