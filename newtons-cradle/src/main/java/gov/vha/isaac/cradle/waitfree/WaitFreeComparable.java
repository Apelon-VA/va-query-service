/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.waitfree;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * 
 * {@code WaitFreeComparable} objects can compare the MD5 checksum of the 
 * original {@code byte[]} data from which they where deserialized 
 * (the deserialization digest), with  
 * currently provided {@code byte[]} data or provided MD5 digest. 
 * This ability to compare original 
 * MD5 checksums with current {@code byte[]} data, enables compare and swap 
 * updates to maps so that they may be updated using wait-free algorithms 
 * (an algorithm where there is guaranteed per-thread progress). Wait-freedom is 
 * the strongest non-blocking guarantee of progress). 
 * @author kec
 */
public interface WaitFreeComparable {

    /**
     * 
     * @return the most significant 64 bits of this objects deserialization MD5 digest. 
     */
    long getMd5Msb();

    /**
     * 
     * @return the least significant 64 bits of this objects deserialization MD5 digest. 
     */
    long getMd5Lsb();

    /**
     * 
     * @param md5Digest the computed digest (represented as an array of 2 primitive
     * long values) to compare against this objects deserialization MD5 digest. 
     * @return true if the digests are equal. 
     */
    default boolean md5Equal(long[] md5Digest){
        return md5Digest[0] == getMd5Msb() && md5Digest[1] == getMd5Lsb();
    }
                
    /**
     * 
     * @param md5DigestBytes the computed digest (represented as a byte[]) 
     * to compare against this objects deserialization MD5 digest.  
     * @return true if the digests are equal. 
     */
    default boolean md5Equal(byte[] md5DigestBytes) {
        assert md5DigestBytes.length == 16 : "digest must be 16 bytes long";
        long msb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (md5DigestBytes[i] & 0xff);
        }
        if (msb != getMd5Msb()) {
            return false;
        }
        long lsb = 0;
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (md5DigestBytes[i] & 0xff);
        }
        return lsb == getMd5Lsb();
    }

    /**
     * 
     * @param input the data for which an MD5 digest is computed, and compared
     * to this objects deserialization digest. 
     * @return true if the digests are equal.
     */
    default boolean verifyDigest(byte[] input) {
        if (input == null && getMd5Lsb() == 0 && getMd5Msb() == 0) {
            return true;
        }
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return md5Equal(md.digest(input));
        } catch (NoSuchAlgorithmException nsae) {
            throw new InternalError("no MD5 support", nsae);
        }

    }

    static long[] digest(byte[] input) {
        if (input == null) {
            return new long[] {0,0};
        }
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] md5Bytes = md.digest(input);
            assert md5Bytes.length == 16 : "digest must be 16 bytes long";
            long[] results = new long[2];
            long msb = 0;
            for (int i = 0; i < 8; i++) {
                msb = (msb << 8) | (md5Bytes[i] & 0xff);
            }
            results[0] = msb;
            long lsb = 0;
            for (int i = 8; i < 16; i++) {
                lsb = (lsb << 8) | (md5Bytes[i] & 0xff);
            }
            results[1] = lsb;
            return results;
        } catch (NoSuchAlgorithmException nsae) {
            throw new InternalError("no MD5 support", nsae);
        }
    }
}
