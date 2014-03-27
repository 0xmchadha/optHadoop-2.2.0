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
import org.apache.hadoop.mapred.CityHash;

public class SharedHashLookup {
    /** The byte length of an integer */
    public static final int INTLENGTH = 4;
	
    /** The byte length of a character */
    public static final int BYTELENGTH = 1;
	
    /** Starting index within the data buffer where data begins */
    //    public static final int STARTINGADDRESS = 8;
    
    /** The length of an address */
    public static final int ADDRESSLENGTH = INTLENGTH;
    
    private static final int numSlots = 4;
    private static final int hashEntryLen = 5;
    private static final int slotSize = hashEntryLen * numSlots;
    private static final int STARTING_ADDRESS = 8;
    private static final int MAX_LEN = 64;
    
    /* Total bytes for Progress */
    private static final Log LOG = LogFactory.getLog(SharedHashMap.class.getName());
    
    MappedByteBuffer shm;
    String fileName;
    private ShmIterator iterator;
    private iterateValues vIter;

    public SharedHashLookup(int num_hashmaps) throws IOException {
        iterator = new ShmIterator();
        vIter = new iterateValues();
    }

    /* this function is synchronized from the above layers */
    public void setNewLookup(String fileName, MappedByteBuffer shm) {
        this.shm = shm;
        this.fileName = fileName;
        iterator.newLookup(shm);
    }
    
    public class shmList {
        String file;
        MappedByteBuffer shm;
    }

    private class ShmIterator implements ShmKVIterator {
        private int hashLoc;
        private int dataLoc;
        private MappedByteBuffer shm;
        private int dataOffset;
        private DataInputBuffer buf;
        private byte[] byteArr;
        private boolean NextKeySame;
	private long hashVal;

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
            
            hashLoc = STARTING_ADDRESS;
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
            
            hashVal = CityHash.cityHash64(keyp, 0, keyLen);
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

        public long getHashVal() {
            return hashVal;
        }
    }

    private boolean keyMatches(MappedByteBuffer shm, int dataAddress, DataInputBuffer key) {
        int valLen = (int)WritableUtils.readIntOpt(shm.get(dataAddress));
        int keyLen = (int)WritableUtils.readIntOpt(shm.get(dataAddress + (1 + valLen + 4)));
	byte[] keyB = key.getData();

        dataAddress += (5 + valLen);
	
        if (keyLen != key.getLength())
            return false;
	
        for (int i = 0; i < keyLen; i++) {
            if (keyB[i] != shm.get(dataAddress + 1 + i))
                return false;
        }
        
        return true;
    }

    
    public class iterateValues {

        private MappedByteBuffer shm;
        private DataInputBuffer key;
        private int dataAddress;
        private DataInputBuffer dib = new DataInputBuffer();
        private byte[] byteArr = new byte[MAX_LEN];
        private long dataOffset;
        private long hashLoc;

        public iterateValues() {
            
        }
        
        public void setKeyBuf(DataInputBuffer key) {
            this.key = key;
        }

        public void setHashMap(MappedByteBuffer shm) {
            this.shm = shm;
        }

        public boolean getKey(long hash) {
            int tag, slot1, slot2, offset;

            dataOffset = shm.getLong(0);
            tag = (byte) (hash >>> 56);
            slot1 = (int) hash & dataOffset;
            slot2 = (int) (slot1 ^ (tag * 0x5bd1e995)) & dataOffset;
            
            offset = slot1 * slotSize + STARTING_ADDRESS;

            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < numSlots; j++) {
                    int loc = offset + j * hashEntryLen;
                    if (tag == shm.get(loc)) {
                        if (keyMatches(shm, dataOffset + shm.getInt(loc + 1), key)) {
                            this.hashLoc = loc;
                            this.dataAddress = dataOffset + shm.getInt(loc+1);
                            return true;
                        }
                    }
                }
                
                offset = slot2 * slotSize + STARTING_ADDRESS;
            }
            
            return false;
        }
        
        public boolean hasNext() {
            if (dataAddress == 0)
                return false;
            
            return true;
        }

        public DataInputBuffer getValue() {
            int valLen = (int)WritableUtils.readIntOpt(shm.get(dataAddress));
            byte[] valp;
            
            valp = byteArr;

            if (valLen > MAX_LEN) {
                byte[] nval = new byte[valLen];
                valp = nval;
            }
            
            for (int i = 0; i < valLen; i++) {
                valp[i] = shm.get(dataAddress + 1 + i);
            }

            dib.reset(valp, 0, valLen);

            DataAddress = shm.getInt(dataAddress + 1 + valLen);
            if (dataAddress != 0)
                dataAddress += dataOffset;
            else {
                // make this key as exhausted by writing 0 to dataaddress for the key
                shm.putInt(loc + 1, 0);
            }
            return dib;
        }
    }

    public ShmKVIterator getIterator() {
        return iterator;
    }

    public iterateValues getVIter() {
        return vIter;
    }

}

