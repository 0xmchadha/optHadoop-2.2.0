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

public class SharedHashCreate {
    private ArrayList<MappedByteBuffer> hma;
    private ArrayList<RandomAccessFile> hmf;
    private ArrayList<MappedByteBuffer> dma;
    private ArrayList<RandomAccessFile> dmf;
    private ArrayList<String> fileName;
    private ArrayList<Long> hashsizes;
    
    private int num_hashmaps;
    private final slotInfo hashSlot1; // first slot for cuckoo hash
    private final slotInfo hashSlot2; // second slot for cuckoo hash
    private final keyInfo nKey; 
    private final keyInfo rKey;
    private final Random generator = new Random();
    private final repinfo repinfo = new repInfo();
    private final ShmIterator iterator;

    /** The byte length of an integer */
    public static final int INTLENGTH = 4;
    /** The byte length of a character */
    public static final int BYTELENGTH = 1;
    /** The length of an address */
    public static final int ADDRESSLENGTH = INTLENGTH;
    private static final int numSlots = 4;
    private static final int hashEntryLen = 5;
    private static final int slotSize = hashEntryLen * numSlots;
    private static final int STARTING_ADDRESS = 8;
    private static final int dataSize = STARTING_ADDRESS + 256*1024*1024;
    /*
      private static final int MAX_CUCKOO = 500;
      private static final int MAX_LEN = 64;
      private byte[] byteArr = new byte[MAX_LEN];
      private DataInputBuffer buf = new DataInputBuffer();
    */
    private static final Log LOG = LogFactory.getLog(SharedHashMap.class.getName());
    
    private class ShmIterator implements ShmKVIterator {
        private int hashLoc;
        private int dataLoc;
        private MappedByteBuffer hma;
        private MappedByteBuffer dma;
        private int hashsize;
        private DataInputBuffer buf;
        private byte[] byteArr;
        private boolean NextKeySame = false;
        private static final int MAX_LEN = 64;
	    
        public ShmIterator() {
            hashLoc = STARTING_ADDRESS;
            buf = new DataInputBuffer();
            byteArr = new byte[MAX_LEN];
        }
	    
        public newIterator(int hashmap_num) {
            hma = SharedHashCreate.this.hma.get(hashmap_num);
            dma = SharedHashCreate.this.dma.get(hashmap_num);
            hashsize = SharedHashCreate.this.hashsizes.get(hashmap_num);
            
            hashLoc = STARTING_ADDRESS;
            dataLoc = 0;
            NextKeySame = false;
        }

        public boolean start(int hashmap_num) {
            int dataAdd;

            while(true) {
                dataAdd = hma.getInt(hashLoc + 1);
                if (dataAdd == 0) {
                    hashLoc += hashEntryLen;
                    if (hashLoc >= hashsize)
                        return false;
                    
                    continue;
                }
		    
                dataLoc = dataAdd;
                return true;
            }
        }
	    
        public boolean next() throws IOException {
            NextKeySame = true;

            if (hashLoc < hashsize) {
                int nDataAdd;
                int valLen;
                int keyLen;
		    
                valLen = (int) WritableUtils.readIntOpt(dma.get(dataLoc));
                nDataAdd = dma.getInt(dataLoc + 1 + valLen);
		    
                if (nDataAdd == 0) {
                    NextKeySame = false;
                    while(true) {
                        hashLoc += hashEntryLen;
                        if (hashLoc >= hashsize)
                            return false;
			    
                        nDataAdd = hma.getInt(hashLoc + 1);
                        if (nDataAdd == 0)
                            continue;
                        dataLoc = nDataAdd;
                        break;
                    }
                } else 
                    dataLoc = nDataAdd;
                return true;
            }
            return false;
        }

        public DataInputBuffer getValue() {
            int valLen = (int)WritableUtils.readIntOpt(dma.get(dataLoc));
            byte[] valp;
		
            valp = byteArr;

            if (valLen > MAX_LEN) {
                byte[] nval = new byte[valLen];
                valp = nval;
            }
            
            for (int i = 0; i < valLen; i++) {
                valp[i] = dma.get(dataLoc + 1 + i);
            }
            
            buf.reset(valp, 0, valLen);
            return buf;
        }

        public DataInputBuffer getKey() {
            int keyLen;
            int dataAdd;
            byte[] keyp;
            int valLen;

            keyp = byteArr;
            dataAdd = hma.getInt(hashLoc + 1);
		
            valLen = (int)WritableUtils.readIntOpt(dma.get(dataAdd));
		
            dataAdd += (1 + valLen + INTLENGTH);
            keyLen = (int)WritableUtils.readIntOpt(dma.get(dataAdd));

            if (keyLen > MAX_LEN) {
                byte[] nKey = new byte[keyLen];
                keyp = nKey;
            }

            for (int i = 0; i < keyLen; i++) {
                keyp[i] = dma.get(dataAdd + 1 + i);
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


    private class repInfo {
        MappedByteBuffer dma;
        MappedByteBuffer hma;
        long hashsize;
        int hashmap_num;
        byte tag;
        int addr;
        slotInfo slot;
    }
        
    private class slotInfo {
	public boolean found;
	public boolean replaced;
	public int hashAddress;
	public int dataAddress;
	public int slot;
    }
    
    private class keyInfo {
	public int slot1;
	public int slot2;
	public byte tag;
	public byte[] Tag;
	public byte[] key;
	public MappedByteBuffer mbf;
	public int len;
	public int keyOffset;
	public long hashMask;
	public boolean reboot = false;
	public boolean check = false;

        public keyInfo() {
            Tag = new byte[1];
        }
        
	public void newHashSize(int hashSize) {
	    hashMask = (hashSize / slotSize) - 1;
	}
	
	public void newKey(MappedByteBuffer mbf, int keyOff, int keyLen) {
	    //   this.key = key;
	    len = keyLen;
	    this.mbf = mbf;
	    check = true;
	    keyOffset = keyOff;
	    long hash = CityHash.MappedcityHash64(mbf, keyOff, keyLen);
	    slot1 = (int) (hash & hashMask);
	    tag = (byte)(hash >>> 56);
	    Tag[0] = tag;
	    slot2 = (int)((slot1 ^ (tag * 0x5bd1e995)) & hashMask);
	}

	public void newKey(byte[] dmba, int keyOff, int keyLen) {
	    //   this.key = key;
	    len = keyLen;
	    this.key = dmba;
	    check = false;
	    keyOffset = keyOff;
	    long hash = CityHash.cityHash64(dmba, keyOff, keyLen);
	    slot1 = (int) (hash & hashMask);
	    tag = (byte)(hash >>> 56);
	    Tag[0] = tag;
	    slot2 = (int)((slot1 ^ (tag * 0x5bd1e995)) & hashMask);
	}

	public int repKey(MappedByteBuffer mbf, byte tag, int valOff, int slot) {
            
	    if (reboot == false) {
		slot1 = slot;
		this.tag = tag;
	    }
	    else {
		int valLen = (int)WritableUtils.readIntOpt(mbf.get(valOff));
		int keyOff = valOff + 1 + valLen + 4;
		int keyLen = (int)WritableUtils.readIntOpt(mbf.get(keyOff));
		long hash = CityHash.MappedcityHash64(mbf, keyOff+1, keyLen);
		slot1 = (int) (hash & hashMask);
		tag = (byte)(hash >>> 56);
	    }
	    
	    Tag[0] = tag;
	    slot2 = (int)((slot1 ^ (tag * 0x5bd1e995)) & hashMask);
	    
	    if (reboot == true) {
		//		System.out.println(slot1 + " " + slot2);
		return slot1;
	    }
	    
	    return slot2;
	}
    }

    private int growHash(int grow, int hashSize) {
        return ((hashSize - STARTING_ADDRESS) * grow + STARTING_ADDRESS);
    }
    
    private void setAvailableAddress(MappedByteBuffer mbf, int val) {
        mbf.putLong(0, val);
    }
    
    public int getAvailableAddress(MappedByteBuffer mbf) {
        return (int) mbf.getLong(0);
    }

    public SharedHashCreate(int num_hashmaps) throws IOException {
        
        this.num_hashmaps = num_hashmaps;

        /* Initialize all the arraylist */
        hma = new ArrayList<MappedByteBuffer>(num_hashmaps);
        hmf = new ArrayList<RandomAccessFile>(num_hashmaps);
        dma = new ArrayList<MappedByteBuffer>(num_hashmaps);
        dmf = new ArrayList<RandomAccessFile>(num_hashmaps);
        fileName = new ArrayList<String>(num_hashmaps);
        hashsizes = new ArrayList<Long>(num_hashmaps);

        /* Initialize other objects required while adding objects */
        nKey = new keyInfo();
        rKey = new keyInfo();
        hashSlot1 = new slotInfo();
        hashSlot2 = new slotInfo();
     
        /* create an iterator for later use */
        iterator = new ShmIterator();
    }
    
    public int setHashMap(String file_name, int hash_size, int uniq_keys) {
        setHashMap(num_hashmaps++, file_name, hash_size, uniq_keys);
        return num_hashmaps - 1;
    }
    
    public void setHashMap(int hashmap_num, String file_name, int hash_size, int uniq_keys) {
        RandomAccessFile hmf, dmf;
        MappedByteBuffer hma, dma;
        
        hmf = new RandomAccessFile(file_name + ".hash", "rw");
        dmf = new RandomAccessFile(file_name + ".data", "rw");

        hmf.setLength(hashSize);
        dmf.setLength(dataSize);
        
        hma = hmf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, hmf.length());
        dma = dmf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, dmf.length());
        this.hma.add(hashmap_num, hma);
        this.dma.add(hashmap_num, dma);
        this.hmf.add(hashmap_num, hmf);
        this.dmf.add(hashmap_num, dmf);
        this.fileName.add(hashmap_num, file_name);
        
        if (hash_size == -1) 
            hash_size = STARTING_ADDRESS + slotSize * uniq_keys;
        
        hashsizes.add(hashmap_num, hash_size);
        
        setAvailableAddress(hma, STARTING_ADDRESS);
        setAvailableAddress(dma, STARTING_ADDRESS);
    }

    public void rename(int hashmap_num) throws IOException {
        RandomAccessFile hmf = this.hmf.get(hashmap_num);
        RandomAccessFile dmf = this.dmf.get(hashmap_num);
        String groupName = this.fileName.get(hashmap_num);

        hmf.close();
        dmf.close();
        File hashFile = new File(groupName + ".hash");
        File hashTmpFile = new File(groupName + ".hash" + ".tmp");
        hashFile.renameTo(hashTmpFile);
        File dataFile = new File(groupName + ".data");
        File dataTmpFile = new File(groupName + ".data" + ".tmp");
        dataFile.renameTo(dataTmpFile);
    }
	
    private boolean keyMatches(MappedByteBuffer dma, int dataAddress, keyInfo key) {
        int valLen = (int)WritableUtils.readIntOpt(dma.get(dataAddress));
        int keyLen = (int)WritableUtils.readIntOpt(dma.get(dataAddress + (1 + valLen + 4)));
	    
        dataAddress += 5 + valLen;
	    
        if (keyLen != key.len) 
            return false;
	    
        if (key.check == false) {
            for (int i = 0; i < keyLen; i++) {
                if (key.key[key.keyOffset + i] != dma.get(dataAddress + 1 + i))
                    return false;
            }
        } else {
            for (int i = 0; i < keyLen; i++) {
                if (key.mbf.get(key.keyOffset + i) != dma.get(dataAddress + 1 + i))
                    return false;
            }
        }
	    
        return true;
    }
	
    private void findEntry(MappedByteBuffer hma, slotInfo slot) {
        int offset = slot.slot * slotSize;	    

        for (int i = 0; i < numSlots; i++) {
            int loc = offset + i * hashEntryLen;
            int dataAddress = hma.getInt(STARTING_ADDRESS + loc + 1);

            if (dataAddress == 0) {
                slot.found = false;
                slot.replaced = false;
                slot.hashAddress = loc;
                return;
            }
        }
        
        slot.found = false;
        slot.replaced = true;
        slot.hashAddress = offset + (Math.abs(generator.nextInt(4))) * hashEntryLen;
        //slot.hashAddress = offset + 2 * hashEntryLen;
    }
	
    private void keyExists(MappedByteBuffer hma, MappedByteBuffer dma, keyInfo key, slotInfo slot) {
        int offset = slot.slot * slotSize;
	   
        for (int i = 0; i < numSlots; i++) {
            int loc = offset + i * hashEntryLen;
            int dataAddress = hma.getInt(STARTING_ADDRESS + loc + 1);
		
            if (dataAddress == 0) {
                slot.found = false;
                slot.replaced = false;
                slot.hashAddress = loc;
                return;
            }
	
            if (key.tag == hma.get(STARTING_ADDRESS + loc)) {
                if (keyMatches(dma, dataAddress, key) == true) {
                    slot.found = true;
                    slot.dataAddress = dataAddress;
                    return;
                } 
            }
        }
	    
        slot.found = false;
        slot.replaced = true;
    }
	
    private int addKV(MappedByteBuffer hma, int keyOff, int keyLen, int valOff, int valLen, byte tag, int hashAddr) {
        
        hma.put(STARTING_ADDRESS + hashAddr, tag);
        hma.putInt(STARTING_ADDRESS + hashAddr + 1, valOff - 1);

        return keyOff + keyLen;
    }
	
    private void addKey(MappedByteBuffer hma, int hashAddr, byte tag, int dataAddr) {
        hma.put(STARTING_ADDRESS + hashAddr, tag);
        hma.putInt(STARTING_ADDRESS + hashAddr + 1, dataAddr);
    }

    private int addNextValue(MappedByteBuffer dma, int NvalOff, int NvalLen, int dataAddr) {
        int valLen = (int)WritableUtils.readIntOpt(dma.get(dataAddr));
        int nextPtr = dma.getInt(dataAddr + valLen + 1);

        dma.putInt(NvalOff + NvalLen, nextPtr);
        dma.putInt(dataAddr + valLen + 1, NvalOff - 1);

        return NvalOff + NvalLen + INTLENGTH;
    }
    
    private boolean cuckooBucket(repInfo rep) {
        MappedByteBuffer dma = rep.dma;
        MappedByteBuffer hma = rep.hma;
        int max_allowed = 0;
        
        while (max_allowed++ < MAX_CUCKOO) {
            // slot gives the info where it 
            // has been evicted from
            
            rep.slot.slot = rKey.repKey(dma, rep.tag, rep.addr, rep.slot.slot);
            rKey.reboot = false;
            
            // store the alternate location 
            // in the slot
            
            // see if this new slot is empty
            findEntry(rep.slot);
            
            if (rep.slot.replaced == false) {
                addKey(hma, rep.slot.hashAddress, rep.tag, rep.addr);
                return true;
            } else {
                byte rtag1 = hma.get(STARTING_ADDRESS + rep.slot.hashAddress);
                int raddr1 = hma.getInt(STARTING_ADDRESS + rep.slot.hashAddress + 1);
                addKey(hma, rep.slot.hashAddress, rep.tag, rep.addr);
                rep.tag = rtag1;
                rep.addr = raddr1;
            }
        }
        return false;
    }
        
    private void rehash(repInfo rep, int growFactor) throws IOException {
        int newHashSize;
        int oldHashSize;
        MappedByteBuffer Lhma, Thma; // large and tmp
        RandomAccessFile Lhmf, Thmf;
        
        oldHashSize = rep.hashsize;
        newHashSize = growHash(growFactor, rep.hashsize);   
        rep.hashsize = newHashSize;
        
        Lhmf = new RandomAccessFile(groupName + "_tmphash", "rw");
        Lhmf.setLength(newHashSize);
        Lhma = Lhmf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Lhmf.length());
	    
        Thmf = this.hmf.get(rep.hashmap_num);
        Thma = this.hma.get(rep.hashmap_num);
	    
        this.hmf.add(rep.hashmap_num, Lhmf);
        this.hma.add(rep.hashmap_num, Lhma);

        rKey.newHashSize(newHashSize - STARTING_ADDRESS);
        nKey.newHashSize(newHashSize - STARTING_ADDRESS);
        rKey.reboot = true;
        cuckooBucket(rep);

        for (int i = 0; i < ((oldHashSize - STARTING_ADDRESS)/slotSize); i++) {
            for (int j = 0; j < numSlots; j++) {
                int addr = STARTING_ADDRESS + i * slotSize + j * hashEntryLen;
                rep.slot.slot = i;
                if (Thma.getInt(addr + 1) != 0) {
                    rep.tag = Thma.get(addr);
                    rep.addr = Thma.getInt(addr + 1);
                    rKey.reboot = true;
                    if (cuckooBucket(rep) == false) {
                        LOG.info("cuckoo hash was not successful even with double the hash size");
                        return;
                    } 
                }
            }
        }
	    
        rKey.reboot = false;
        this.hashsizes.set(rep.hashmap_num, newHashSize);
        File file = new File(groupName + ".hash");
        file.delete();
        new File(groupName + "_tmphash").renameTo(file);
    }

    public void put(int hashmap_num, int valOff, int valLen, int keyOff, int keyLen) throws IOException {
        MappedByteBuffer hma, dma;
        boolean found = false;
        boolean replaced = true;
        boolean checkOther = false;
        slotInfo slot = null;
        int random;
        int randHashEntry;
        int hashsize;
        
        hma = this.hma.get(hashmap_num);
        dma = this.dma.get(hashmap_num);
        hashsize = hashsizes.get(hashmap_num);
        nKey.newHashSize(hashsize);
        rKey.newHashSize(hashsize);
            
        nKey.newKey(dma, keyOff, keyLen);
        hashSlot1.slot = nKey.slot1;
        hashSlot2.slot = nKey.slot2;

        keyExists(hma, dma, nKey, hashSlot1);
	    
        if (hashSlot1.found == true) {
            slot = hashSlot1;
            found = true;
        } else {
            keyExists(hma, dma, nKey, hashSlot2);
            if (hashSlot2.found == true) {
                slot = hashSlot2;
                found = true;
            }
        }
	    
        if (found == true) {
            setAvailableAddress(dma, addNextValue(dma, valOff, valLen, slot.dataAddress));
            return;
        }
	    
        random = Math.abs(generator.nextInt(2));
        //	    random = 1;

        if (hashSlot1.replaced == false) {
            slot = hashSlot1;
            replaced = false;
            checkOther = true;
        }
	    
        if (hashSlot2.replaced == false) {
            if (checkOther == false || random == 1) {
                slot = hashSlot2;
                replaced = false;
            }		
        }
	    
        if (replaced == false) {
            setAvailableAddress(dma, addKV(hma, keyOff, keyLen, valOff, valLen, nKey.tag, slot.hashAddress));
            return;
        }
	    
        randHashEntry = Math.abs(generator.nextInt(4));
        //   randHashEntry = 2;

        if (random == 0)
            slot = hashSlot1;
        else			       
            slot = hashSlot2;
	    
        int addr = slot.slot * slotSize + randHashEntry * hashEntryLen;
        byte rtag = hma.get(STARTING_ADDRESS + addr);
        int rDaddr = hma.getInt(STARTING_ADDRESS + addr + 1);
        int max_allowed = 0;

        setAvailableAddress(dma, addKV(hma, keyOff, keyLen, valOff, valLen, nKey.tag, addr));
        
        repinfo.hma = hma;
        repinfo.dma = dma;
        repinfo.hashsize = hashsize;
        repinfo.hashmap_num = hashmap_num;
        repinfo.tag = rtag;
        repinfo.addr = rDaddr;
        repinfo.slot = slot;

        if (cuckooBucket(repinfo) == false) {
            LOG.info("Doubling");
            rehash(repinfo, 2);
            //            System.gc();
        }
    }

    private DataInputBuffer retValBuf(int dataLoc) {
        byte[] valp;
	    
        int valLen = (int)WritableUtils.readIntOpt(dma.get(dataLoc));
	  
        valp = byteArr;
        if (valLen > MAX_LEN) {
            byte[] nval = new byte[valLen];
            valp = nval;
        }
	    
        for (int i = 0; i < valLen; i++) {
            valp[i] = dma.get(dataLoc + 1 + i);
        }
        
        buf.reset(valp, 0, valLen);

        return buf;
    }


    private DataInputBuffer get(int hashmap_num, DataInputBuffer key) {
        byte[] keybuf = key.getData();
        int keyLength = key.getLength();
	
        nKey.newHashSize(this.hashsizes.get(hashmap_num));
        nKey.newKey(keybuf, 0, keyLength);
        
        hashSlot1.slot = nKey.slot1;
        keyExists(nKey, hashSlot1);
	
        if (hashSlot1.found == true) {
            return retValBuf(hashSlot1.dataAddress);
        }
	
        hashSlot1.slot = nKey.slot2;
        keyExists(nKey, hashSlot1);
	
        if (hashSlot1.found == true) {
            return retValBuf(hashSlot1.dataAddress);
        }
        return null;
    }

    public long getHashSize(int hashmap_num) {
        return this.hashsizes.get(hashmap_num);
    }

    public long getRawLength(int hashmap_num) {
        return getAvailableAddress(this.dma.get(hashmap_num)) + this.hashsizes.get(hashmap_num);
    }

    public ShmKVIterator getIterator() {
        return iterator;
    }

    public void setIterator(int hashmap_num) {
        iterator.newIterator(hashmap_num);
    }
    
    public int getOffset(int hashmap_num) {
        return getAvailableAddress(this.dma.get(hashmap_num));
     }

    public MappedByteBuffer getMappedByteBuf(int hashmap_num) {
        return this.dma.get(hashmap_num);
    }
    
    public void replaceWriter(int i, int j) {
        hma.set(i, hmf.get(j));
        hmf.set(i, hmf.get(j));
        dma.set(i, dma.get(j));
        dmf.set(i, dmf.get(j));
        fileName.set(i, fileName.get(j));
        hashsizes.set(i, fileName.get(j));
    }
        
    public void close(int hashmap_num) {
        hma.get(hashmap_num).putLong(0, hashsizes.get(hashmap_num));
    }
}

