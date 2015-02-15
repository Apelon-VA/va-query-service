/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.vha.isaac.cradle.log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author kec
 */
public enum LogEntry {

    CONCEPT((byte) 1), COMMIT_RECORD((byte) 2);

    private final byte token;

    private LogEntry(byte token) {
        this.token = token;
    }

    public static LogEntry fromDataStream(DataInput input) throws IOException {
        byte token = input.readByte();
        switch (token) {
            case 1:
                return CONCEPT;
            case 2:
                return COMMIT_RECORD;
            default:
                throw new UnsupportedOperationException("Can't handle: " + token);
        }
    }

    public void toDataStream(DataOutput out) throws IOException {
        out.writeByte(token);
    }
}
