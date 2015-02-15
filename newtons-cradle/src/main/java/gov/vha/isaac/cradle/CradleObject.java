/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 *
 * @author kec
 */
public interface CradleObject {

    long getMd5Msb();

    long getMd5Lsb();

    default boolean md5Equal(long[] md5Data){
        return md5Data[0] == getMd5Msb() && md5Data[1] == getMd5Lsb();
    }
                
    default boolean md5Equal(byte[] md5Bytes) {
        assert md5Bytes.length == 16 : "digest must be 16 bytes long";
        long msb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (md5Bytes[i] & 0xff);
        }
        if (msb != getMd5Msb()) {
            return false;
        }
        long lsb = 0;
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (md5Bytes[i] & 0xff);
        }
        return lsb == getMd5Lsb();
    }

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
