package edu.uci.ics.cs221.index.inverted;

import java.util.ArrayList;
import java.util.List;

/**
 * Implement this compressor with Delta Encoding and Variable-Length Encoding.
 * See Project 3 description for details.
 */
public class DeltaVarLenCompressor implements Compressor {

    private final int ONE_BYTE_BOUNDARY = 128;
    private final int TWO_BYTE_BOUNDARY = 16384;
    private final int THREE_BYTE_BOUNDARY = TWO_BYTE_BOUNDARY * ONE_BYTE_BOUNDARY;
    private final int FOUR_BYTE_BOUNDARY = TWO_BYTE_BOUNDARY * TWO_BYTE_BOUNDARY;

    @Override
    public byte[] encode(List<Integer> integers) {

        /**
         * Build delta list.
         */
        List<Integer> deltas = new ArrayList<>();
        deltas.add(integers.get(0));
        for (int i = 1; i < integers.size(); i++) {
            deltas.add(integers.get(i) - integers.get(i - 1));
        }

        /**
         * Initialize compressed bytes array.
         */
        int compressedLength = 0;
        for (int delta : deltas) {
            if (delta < ONE_BYTE_BOUNDARY) compressedLength += 1;
            if (delta >= ONE_BYTE_BOUNDARY && delta < TWO_BYTE_BOUNDARY) compressedLength += 2;
            if (delta >= TWO_BYTE_BOUNDARY && delta < THREE_BYTE_BOUNDARY) compressedLength += 3;
            if (delta >= THREE_BYTE_BOUNDARY && delta < FOUR_BYTE_BOUNDARY) compressedLength += 4;
            if (delta >= FOUR_BYTE_BOUNDARY) compressedLength += 5;
        }
        byte[] compressed = new byte[compressedLength];

        /**
         * Fill compressed bytes array with data.
         */
        int offset = 0;
        for (int delta : deltas) {
            byte[] deltaCmp = compress(delta);

            if (delta < ONE_BYTE_BOUNDARY) {
                compressed[offset++] = deltaCmp[0];
            }

            if (delta >= ONE_BYTE_BOUNDARY && delta < TWO_BYTE_BOUNDARY) {
                for (int i = 0; i < 2; i++) compressed[offset++] = deltaCmp[i];
            }

            if (delta >= TWO_BYTE_BOUNDARY && delta < THREE_BYTE_BOUNDARY) {
                for (int i = 0; i < 3; i++) compressed[offset++] = deltaCmp[i];
            }

            if (delta >= THREE_BYTE_BOUNDARY && delta < FOUR_BYTE_BOUNDARY) {
                for (int i = 0; i < 4; i++) compressed[offset++] = deltaCmp[i];
            }

            if (delta >= FOUR_BYTE_BOUNDARY) {
                for (int i = 0; i < 5; i++) compressed[offset++] = deltaCmp[i];
            }
        }

        return compressed;
    }

    private byte[] compress(int delta) {
        byte[] deltaBytes;

        if (delta < ONE_BYTE_BOUNDARY) {

            deltaBytes = new byte[1];
            deltaBytes[0] = (byte)delta;

        } else if (delta >= ONE_BYTE_BOUNDARY && delta < TWO_BYTE_BOUNDARY) {

            deltaBytes = new byte[2];
            deltaBytes[0] = (byte)(ONE_BYTE_BOUNDARY + delta / ONE_BYTE_BOUNDARY);
            delta -= ONE_BYTE_BOUNDARY * (delta / ONE_BYTE_BOUNDARY);
            deltaBytes[1] = compress(delta)[0];

        } else if (delta >= TWO_BYTE_BOUNDARY && delta < THREE_BYTE_BOUNDARY) {

            deltaBytes = new byte[3];
            deltaBytes[0] = (byte)(ONE_BYTE_BOUNDARY + delta / TWO_BYTE_BOUNDARY);
            delta -= TWO_BYTE_BOUNDARY * (delta / TWO_BYTE_BOUNDARY);
            deltaBytes[1] = (byte)(ONE_BYTE_BOUNDARY + delta / ONE_BYTE_BOUNDARY);
            delta -= ONE_BYTE_BOUNDARY * (delta / ONE_BYTE_BOUNDARY);
            deltaBytes[2] = compress(delta)[0];

        } else if (delta >= THREE_BYTE_BOUNDARY && delta < FOUR_BYTE_BOUNDARY) {

            deltaBytes = new byte[4];
            deltaBytes[0] = (byte)(ONE_BYTE_BOUNDARY + delta / THREE_BYTE_BOUNDARY);
            delta -= THREE_BYTE_BOUNDARY * (delta / THREE_BYTE_BOUNDARY);
            deltaBytes[1] = (byte)(ONE_BYTE_BOUNDARY + delta / TWO_BYTE_BOUNDARY);
            delta -= TWO_BYTE_BOUNDARY * (delta / TWO_BYTE_BOUNDARY);
            deltaBytes[2] = (byte)(ONE_BYTE_BOUNDARY + delta / ONE_BYTE_BOUNDARY);
            delta -= ONE_BYTE_BOUNDARY * (delta / ONE_BYTE_BOUNDARY);
            deltaBytes[3] = (byte)delta;

        } else {

            deltaBytes = new byte[5];
            deltaBytes[0] = (byte)(ONE_BYTE_BOUNDARY + delta / FOUR_BYTE_BOUNDARY);
            delta -= FOUR_BYTE_BOUNDARY * (delta / FOUR_BYTE_BOUNDARY);
            deltaBytes[1] = (byte)(ONE_BYTE_BOUNDARY + delta / THREE_BYTE_BOUNDARY);
            delta -= THREE_BYTE_BOUNDARY * (delta / THREE_BYTE_BOUNDARY);
            deltaBytes[2] = (byte)(ONE_BYTE_BOUNDARY + delta / TWO_BYTE_BOUNDARY);
            delta -= TWO_BYTE_BOUNDARY * (delta / TWO_BYTE_BOUNDARY);
            deltaBytes[3] = (byte)(ONE_BYTE_BOUNDARY + delta / ONE_BYTE_BOUNDARY);
            delta -= ONE_BYTE_BOUNDARY * (delta / ONE_BYTE_BOUNDARY);
            deltaBytes[4] = (byte)(delta);

        }

        return deltaBytes;
    }

    @Override
    public List<Integer> decode(byte[] bytes, int start, int length) {
        List<Integer> result = new ArrayList<>();

        List<Byte> byteList = new ArrayList<>();
        for (int i = start; i < start + length; i++) {
            if ((int)bytes[i] < 0) {
                byteList.add(bytes[i]);
            }
            if ((int)bytes[i] >= 0) {
                byteList.add(bytes[i]);
                if (byteList.size() == 1) {
                    result.add((int)byteList.get(0));
                }
                if (byteList.size() == 2) {
                    result.add((byteList.get(0) - (byte)ONE_BYTE_BOUNDARY) * ONE_BYTE_BOUNDARY
                            + (int)byteList.get(1));
                }
                if (byteList.size() == 3) {
                    result.add((byteList.get(0) - (byte)ONE_BYTE_BOUNDARY) * TWO_BYTE_BOUNDARY
                            + (byteList.get(1) - (byte)ONE_BYTE_BOUNDARY) * ONE_BYTE_BOUNDARY
                            + (int)byteList.get(2));
                }
                if (byteList.size() == 4) {
                    result.add((byteList.get(0) - (byte)ONE_BYTE_BOUNDARY) * THREE_BYTE_BOUNDARY
                            + (byteList.get(1) - (byte)ONE_BYTE_BOUNDARY) * TWO_BYTE_BOUNDARY
                            + (byteList.get(2) - (byte)ONE_BYTE_BOUNDARY) * ONE_BYTE_BOUNDARY
                            + (int)byteList.get(3));
                }
                if (byteList.size() == 5) {
                    result.add((byteList.get(0) - (byte)ONE_BYTE_BOUNDARY) * FOUR_BYTE_BOUNDARY
                            + (byteList.get(1) - (byte)ONE_BYTE_BOUNDARY) * THREE_BYTE_BOUNDARY
                            + (byteList.get(2) - (byte)ONE_BYTE_BOUNDARY) * TWO_BYTE_BOUNDARY
                            + (byteList.get(3) - (byte)ONE_BYTE_BOUNDARY) * ONE_BYTE_BOUNDARY
                            + (int)byteList.get(4));
                }
                byteList.clear();
            }
        }

        for (int i = 1; i < result.size(); i++) {
            result.set(i, result.get(i) + result.get(i - 1));
        }

        return result;
    }

}