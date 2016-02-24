package bde.SparkPatternDetector;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashGenerator {

	private HashGenerator() {

	}

	public static String generateMD5(String valueA, String valueB) throws HashGenerationException {
		return hashString(valueA, valueB, "MD5");
	}

	private static String hashString(String artist, String title, String algorithm) throws HashGenerationException {

		try {
			MessageDigest digest = MessageDigest.getInstance(algorithm);
			byte[] hashedBytes = digest.digest((artist+title).getBytes("UTF-8"));

			return convertByteArrayToHexString(hashedBytes);
		} catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
			throw new HashGenerationException("Hash vom String kann nicht erzeugt werden", ex);
		}
	}

	private static String convertByteArrayToHexString(byte[] arrayBytes) {
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < arrayBytes.length; i++) {
			stringBuffer.append(Integer.toString((arrayBytes[i] & 0xff) + 0x100, 16).substring(1));
		}
		return stringBuffer.toString();
	}

}
