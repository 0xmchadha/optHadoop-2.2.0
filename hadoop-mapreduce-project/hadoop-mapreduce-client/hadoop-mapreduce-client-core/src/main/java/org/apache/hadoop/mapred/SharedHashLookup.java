/* Author: Mehul Chadha
 *
 * This work is a modification of the older sharedhashmap I wrote, to 
 * reduce the amount of memory required. 
 * 
 */

package org.apache.hadoop.mapred;

import java.io.*;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.io.PrintStream;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.ShmKVIterator;
import org.apache.hadoop.mapred.CityHash;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.io.WritableUtils;

public class SharedHashLookup {
    MappedByteBuffer shf;
    String fileName;
    private ShmIterator iterator;
    
    public SharedHashLookup(int num_hashmaps) throws IOException {
        iterator = new ShmIterator();
    }

    /* this function is synchronized from the above layer */
    public int setNewLookup(String fileName, MappedByteBuffer shm) {
        this.shm = shm;
        this.fileName = fileName;
        iterator.newLookup(shm);
    }

    private class ShmIterator implements ShmKVIterator {
        private int hashLoc;
        private int dataLoc;
        private MappedByteBuffer shm;
        private int dataOffset;
        private DataInputBuffer buf;
        private byte[] byteArr;
        private boolean NextKeySame;
        private static final int MAX_LEN = 64;
	    
        public ShmIterator() {
            buf = new DataInputBuffer();
            byteArr = new byte[MAX_LEN];
        }

        public void newLookup(MappedByteBuffer shm) {
            this.shm = shm;
        }

        public boolean start() {
            int dataAdd;
            int keyLen;
            
            hashLoc = STARTING_ADRESS;
            dataOffset = (int) shm.getLong(0);

            while(true) {
                dataAdd = shm.getInt(hashLoc + 1);
                if (dataAdd == 0) {
                    hashLoc += hashEntryLen;
                    if (hashLoc >= dataOffset)
                        return false;
			
                    continue;
                }

                dataLoc = dataOffset + dataAdd;
                return true;
            }
        }
	    
        public boolean next() throws IOException {
            NextKeySame = true;

            if (hashLoc < dataOffset) {
                int nDataAdd;
                int valLen;
                int keyLen; 
		   
                valLen = (int)WritableUtils.readIntOpt(shm.get(dataLoc));
                nDataAdd = shm.getInt(dataLoc + 1 + valLen); //next dataAddress
                if (nDataAdd == 0) {
                    NextKeySame = false;
                    while(true) {
                        hashLoc += hashEntryLen;
                        if (hashLoc >= dataOffset)
                            return false;
                        if ((nDataAdd = shm.getInt(hashLoc + 1)) == 0)
                            continue;

                        dataLoc = dataOffset + nDataAdd;
                        break;
                    }
                } else {
                    dataLoc = dataOffset + nDataAdd;
                }
                return true;
            }
            return false;
        }

        public DataInputBuffer getValue() {
            int valLen = (int)WritableUtils.readIntOpt(shm.get(dataLoc));
            byte[] valp;
		
            valp = byteArr;

            if (valLen > MAX_LEN) {
                byte[] nval = new byte[valLen];
                valp = nval;
            }
		
            for (int i = 0; i < valLen; i++) {
                valp[i] = shm.get(dataLoc + 1 + i);
            }

            buf.reset(valp, 0, valLen);
            return buf;
        }

        public DataInputBuffer getKey() {
            int valLen;
            int dataAdd;
            byte[] keyp;
            int keyLen;

            keyp = byteArr;
            dataAdd = shm.getInt(hashLoc + 1);

            valLen = (int)WritableUtils.readIntOpt(shm.get(dataOffset + dataAdd));
            dataAdd += (dataOffset + 1 + valLen + INTLENGTH);
            keyLen = (int)WritableUtils.readIntOpt(shm.get(dataAdd));

            if (keyLen > MAX_LEN) {
                byte[] nKey = new byte[keyLen];
                keyp = nKey;
            }

            for (int i = 0; i < keyLen; i++) {
                keyp[i] = shm.get(dataAdd + 1 + i);
            }

            buf.reset(keyp, 0, keyLen);
            return buf;
        }
	    
        public boolean isNextKeySame() {
            return NextKeySame;
        }
	    
        public Progress getProgress() {
            return null;
        }
        public void close() {
                
        }
    }
    
    public ShmKVIterator getIterator(int hashmap_num) {
        return iterator;
    }
}


