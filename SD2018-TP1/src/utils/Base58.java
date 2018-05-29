package utils;

import com.chrylis.codec.base58.Base58Codec;

final public class Base58 {

	public static String encode( String str ) {
		return Base58Codec.doEncode( str.getBytes() );
	}
	
	public static String encode( byte[] bytes ) {
		return Base58Codec.doEncode( bytes );
	}
	
	public static String decode( String str ) {
		return new String(Base58Codec.doDecode( str ));
	}
}
