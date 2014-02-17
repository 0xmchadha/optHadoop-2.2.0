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

/**
 * HashMap implementation that passes calls onto a memory-mapped file system.
 * For sharing a single HashMap amongst JVM instances on a single machine
 * 
 * @author Caleb Solano
 * @author Mehul Chadha
 * @author Ruoyu Liu
 *
 */

/*
  Key Store
                     --------------- -------
  5 bytes per key    1byte hashCode |  ptr
                     --------------- -------
  4 key entries per slot

  Val Store

  --------- ------
  val       ptr
  --------- ------
  4byte + (val length) + (ptr len)
*/

public class SharedHashMap { //implements Map<DataInputBuffer, DataInputBuffer>, MultiValueMap<DataInputBuffer, DataInputBuffer> {
    
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
    
    private String groupName;
    private boolean shmMode; // either in creation or in lookup mode

    private createHash createCuckoo;
    private lookupHash lookupCuckoo;
    /* Total bytes for Progress */
    private int TotalBytes = 0;
    private static final Log LOG = LogFactory.getLog(SharedHashMap.class.getName());
    
    private byte getTag(byte[] key, int keyLen) {
	int hash=7;
	for (int i=0; i < keyLen; i++) {
	    hash = (hash*31) ^ (key[i] & 0xff);
	}
	return (byte) (Math.abs(hash) % 256);
    }

    private long hashfunc(byte[] key, int len) {
	long hval = 33554467;
	
	for (int i = 0; i < len; i++) {
	    hval *= 0x1000193;
	    hval ^= (key[i] & 0xff);
	}
	
	return hval;
    }
	
    private long hashfunc(MappedByteBuffer mbf, int offset) {
	int len = mbf.getInt(offset);
	offset += 4;
	long hval = 33554467;
	
	for (int i = 0; i < len; i++) {
	    hval *= 0x1000193;
	    hval ^= (mbf.get(offset + i) & 0xff);
	}
	
	return hval;
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

	public keyInfo(int hashSize) {
	    Tag = new byte[1];
	    hashMask = (hashSize / slotSize) - 1;
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
    
    /* This hash is based on Cuckoo Hashing */
    private class createHash {
	private MappedByteBuffer hma;
	private RandomAccessFile hmf;
	public MappedByteBuffer dma;
	private RandomAccessFile dmf;
	/*
 	 * ---------------------
	 * | 5b |    |    |    |
	 * |-------------------|
	 * |    |    |    |    |               
	 * |-------------------|
	 * |    |    |    |    |
	 * |-------------------|
	 * |    |    |    |    |
	 * ---------------------
	 */
	private static final int MAX_CUCKOO = 500;
	private int hashSize;
	//	private int hashSize =  STARTING_ADDRESS + 65536 * slotSize*4; // grows in multiples of 2
	private int dataSize = STARTING_ADDRESS + 256*1024*1024; // 256MB is an upperbound
	private keyInfo nKey; // new key
	private keyInfo rKey; // replaced key
	private slotInfo hashSlot1; //gives details about the slot in the hash
	private slotInfo hashSlot2; 
	private String groupName;
	private static final int MAX_LEN = 64;
	private byte[] byteArr = new byte[MAX_LEN];
	private DataInputBuffer buf = new DataInputBuffer();
	
	Random generator = new Random();
	private int numKeys = 0;
	private repInfo repinfo = new repInfo();

	private int growHash(int grow) {
	    return ((hashSize - STARTING_ADDRESS) * grow + STARTING_ADDRESS);
	}
	
	private void setAvailableAddress(MappedByteBuffer mbf, int val) {
	    mbf.putLong(0, val);
	}
	
	public int getAvailableAddress(MappedByteBuffer mbf) {
	    return (int) mbf.getLong(0);
	}
	
	private class repInfo {
	    byte tag;
	    int addr;
	    slotInfo slot;
	}

	public createHash(String fileName, long hash_size, int uniq_keys) throws IOException {
	    groupName = fileName;
	    hmf = new RandomAccessFile(fileName + ".hash", "rw");
	    dmf = new RandomAccessFile(fileName + ".data", "rw");
	    if (hash_size != -1)
		hashSize = (int) hash_size;
	    else
		hashSize = STARTING_ADDRESS + slotSize * uniq_keys;

	    hmf.setLength(hashSize);
	    dmf.setLength(dataSize);
	    hma = hmf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, hmf.length());
	    dma = dmf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, dmf.length());
	    nKey = new keyInfo(hashSize - STARTING_ADDRESS);
	    rKey = new keyInfo(hashSize - STARTING_ADDRESS);
	    hashSlot1 = new slotInfo();
	    hashSlot2 = new slotInfo();
	    setAvailableAddress(hma, STARTING_ADDRESS);
	    setAvailableAddress(dma, STARTING_ADDRESS);
	}
	
	public void rename() throws IOException{
	    hmf.close();
	    dmf.close();
	    File hashFile = new File(groupName + ".hash");
	    File hashTmpFile = new File(groupName + ".hash" + ".tmp");
	    hashFile.renameTo(hashTmpFile);
	    File dataFile = new File(groupName + ".data");
	    File dataTmpFile = new File(groupName + ".data" + ".tmp");
	    dataFile.renameTo(dataTmpFile);
	}
	
	private boolean keyMatches(int dataAddress, keyInfo key) {
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
	
	private void findEntry(slotInfo slot) {
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
	
	private void keyExists(keyInfo key, slotInfo slot) {
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
		    if (keyMatches(dataAddress, key) == true) {
			slot.found = true;
			slot.dataAddress = dataAddress;
			return;
		    } 
		}
	    }
	    
	    slot.found = false;
	    slot.replaced = true;
	}
	
	private int addKV(int keyOff, int keyLen, int valOff, int valLen,
			  byte tag, int hashAddr) {
	    
	    hma.put(STARTING_ADDRESS + hashAddr, tag);
	    hma.putInt(STARTING_ADDRESS + hashAddr + 1, valOff - 1);

	    return keyOff + keyLen;
	}
	
	private void addKey(int hashAddr, byte tag, int dataAddr) {
	    hma.put(STARTING_ADDRESS + hashAddr, tag);
	    hma.putInt(STARTING_ADDRESS + hashAddr + 1, dataAddr);
	}

	private int addNextValue(int NvalOff, int NvalLen, int dataAddr) {
	    int valLen = (int)WritableUtils.readIntOpt(dma.get(dataAddr));
	    int nextPtr = dma.getInt(dataAddr + valLen + 1);

	    dma.putInt(NvalOff + NvalLen, nextPtr);
	    dma.putInt(dataAddr + valLen + 1, NvalOff - 1);

	    return NvalOff + NvalLen + INTLENGTH;
	}
	
	private boolean cuckooBucket(repInfo rep) {
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
		    addKey(rep.slot.hashAddress, rep.tag, rep.addr);
		    return true;
		} else {
		    byte rtag1 = hma.get(STARTING_ADDRESS + rep.slot.hashAddress);
		    int raddr1 = hma.getInt(STARTING_ADDRESS + rep.slot.hashAddress + 1);
		    addKey(rep.slot.hashAddress, rep.tag, rep.addr);
		    rep.tag = rtag1;
		    rep.addr = raddr1;
		}
	    }
	    
	    return false;
	}

	private void rehash(repInfo rep, int growFactor) throws IOException {
	    int newHashSize;
	    MappedByteBuffer Lhma, Thma; // large and tmp
	    RandomAccessFile Lhmf, Thmf;
	    newHashSize = growHash(growFactor);	    
	    Lhmf = new RandomAccessFile(groupName + "_tmphash", "rw");
	    Lhmf.setLength(newHashSize);
	    Lhma = Lhmf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Lhmf.length());
	    
	    Thmf = hmf;
	    Thma = hma;
	    
	    hmf = Lhmf;
	    hma = Lhma;

	    rKey.newHashSize(newHashSize - STARTING_ADDRESS);
	    nKey.newHashSize(newHashSize - STARTING_ADDRESS);
	    rKey.reboot = true;
	    
	    cuckooBucket(rep);

	    for (int i = 0; i < ((hashSize - STARTING_ADDRESS)/slotSize); i++) {
		for (int j = 0; j < numSlots; j++) {
		    int addr = STARTING_ADDRESS + i * slotSize + j * hashEntryLen;
		    rep.slot.slot = i;
		    if (Thma.getInt(addr + 1) != 0) {
			rep.tag = Thma.get(addr);
			rep.addr = Thma.getInt(addr + 1);
			rKey.reboot = true;
			if (cuckooBucket(rep) == false) {
			    //			    new File("tmp_hash").delete();
			    //			    hmf = Thmf;
			    //			    hma = Thma;
			    //			    rehash(rtag, rDaddr, slot, growFactor * 2);
			    //			    return;
			    LOG.info("cuckoo hash was not successful even with double the hash size");
			    return;
			} 
		    }
		}
	    }
	    
	    rKey.reboot = false;
	    hashSize = newHashSize;
	    File file = new File(groupName + ".hash");
	    file.delete();
	    new File(groupName + "_tmphash").renameTo(file);
	}
	
	/*	private void test() throws IOException {
	    ShmKVIterator iter = getIterator();
	    DataInputBuffer test_buf;
	    boolean nextKeyIsSame;

	    iter.start();

	    do {
		nextKeyIsSame = iter.isNextKeySame();
		if (nextKeyIsSame == false)
		    test_buf = iter.getKey();
		//		test_buf = iter.getValue();
	    } while(iter.next());
		
	}
	*/
	private void put(int valOff, int valLen, int keyOff, int keyLen) throws IOException {

	    boolean found = false;
	    boolean replaced = true;
	    boolean checkOther = false;
	    //	    int availableAddress = getAvailableAddress(dma);
	    slotInfo slot = null;
	    int random;
	    int randHashEntry;
	    
	    numKeys++;
	    nKey.newKey(dma, keyOff, keyLen);
	    hashSlot1.slot = nKey.slot1;
	    hashSlot2.slot = nKey.slot2;

	    keyExists(nKey, hashSlot1);
	    
	    if (hashSlot1.found == true) {
		slot = hashSlot1;
		found = true;
	    } else {
		keyExists(nKey, hashSlot2);
		if (hashSlot2.found == true) {
		    slot = hashSlot2;
		    found = true;
		}
	    }
	    
	    if (found == true) {
		setAvailableAddress(dma, addNextValue(valOff, valLen, slot.dataAddress));
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
		setAvailableAddress(dma, addKV(keyOff, keyLen, valOff, valLen, nKey.tag, slot.hashAddress));
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
	    setAvailableAddress(dma, addKV(keyOff, keyLen, valOff, valLen, nKey.tag, addr));
	    int max_allowed = 0;

	    repinfo.tag = rtag;
	    repinfo.addr = rDaddr;
	    repinfo.slot = slot;

	    if (cuckooBucket(repinfo) == false) {
		LOG.info("Doubling");
		rehash(repinfo, 2);
		System.gc();
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

	private DataInputBuffer get(DataInputBuffer key) {
	    byte[] keybuf = key.getData();
	    int keyLength = key.getLength();
	    
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
	
	private class ShmIterator implements ShmKVIterator {
	    private int hashLoc;
	    private int dataLoc;
	    DataInputBuffer buf;
	    byte[] byteArr;
	    boolean NextKeySame = false;
	    private static final int MAX_LEN = 64;
	    
	    public ShmIterator() {
		hashLoc = STARTING_ADDRESS;
		buf = new DataInputBuffer();
		byteArr = new byte[MAX_LEN];
	    }
	    
	    public boolean start() {
		int dataAdd;

		while(true) {
		    dataAdd = hma.getInt(hashLoc + 1);
		    if (dataAdd == 0) {
			hashLoc += hashEntryLen;
			if (hashLoc >= hashSize)
			    return false;
			
			continue;
		    }
		    
		    dataLoc = dataAdd;
		    return true;
		}
	    }
	    
	    public boolean next() throws IOException {
		NextKeySame = true;

		if (hashLoc < hashSize) {
		    int nDataAdd;
		    int valLen;
		    int keyLen;
		    
		    valLen = (int) WritableUtils.readIntOpt(dma.get(dataLoc));
		    nDataAdd = dma.getInt(dataLoc + 1 + valLen);
		    
		    if (nDataAdd == 0) {
			NextKeySame = false;
			while(true) {
			    hashLoc += hashEntryLen;
			    if (hashLoc >= hashSize)
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

	public ShmKVIterator getIterator() {
	    return new ShmIterator();
	}

	public void close() {
	    hma.putLong(0, hashSize); // hashSize = actual hashSize and the first 8 bytes. 
	}

	public long getRawLength() {
	    return getAvailableAddress(dma) + hashSize;
	}
	public long getHashSize() {
	    return hashSize;
	}
    }
    
    private class lookupHash {
	private MappedByteBuffer shm;
	private RandomAccessFile shf;
	private String fileName;
	private int dataOffset;
	
	public lookupHash(String file) throws IOException {
	    if (new File(file).exists())
		LOG.info("file exists");
	    else
		LOG.info("doesnt exists");

	    /*	    Random randomGenerator = new Random();
	    int m = 0;
	    long ik = 0;
	    
	    while(ik < 99999999999999L) {
		m = randomGenerator.nextInt(100);
		ik++;
	    }
	    
	    if (m < 50)
		return;
	    */
	    fileName = file;
	    shf = new RandomAccessFile(file, "r");
	    shm = shf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, shf.length());
	    shm.load(); 
	    dataOffset = (int) shm.getLong(0);
	}
	
	public void destroy() {
	    new File(fileName).delete();
	}
	
	private class ShmIterator implements ShmKVIterator {
	    private int hashLoc;
	    private int dataLoc;
	    DataInputBuffer buf;
	    byte[] byteArr;
	    boolean NextKeySame;
	    private static final int MAX_LEN = 64;
	    
	    public ShmIterator() {
		hashLoc = STARTING_ADDRESS;
		buf = new DataInputBuffer();
		byteArr = new byte[MAX_LEN];
	    }

	    public boolean start() {
		int dataAdd;
		int keyLen;
		
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

	public ShmKVIterator getIterator() {
	    return new ShmIterator();
	}
    }

    public SharedHashMap(String shmFile, boolean isCreating, int pred_keys) {
	try {
	    if (isCreating == true) {
		createCuckoo = new createHash(shmFile, -1, pred_keys);
	    } else {
		/*		Random randomGenerator = new Random();
		int m = 0;
		long ik = 0;
		
		while(ik < 99999999999999L) {
		    m = randomGenerator.nextInt(100);
		    ik++;
		}
		
		if (m < 50)
		    return;
		*/
		//		LOG.info("creating shm");
		lookupCuckoo = new lookupHash(shmFile);
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }
    
    public SharedHashMap(String shmFile, long hash_size) throws IOException {
	createCuckoo = new createHash(shmFile, hash_size, 0);
    }

    //    public void put(DataInputBuffer key, DataInputBuffer value) throws IOException {
	//	createCuckoo.put(key, value);
    //    }

    public void put(int valOff, int valLen, int keyOff, int keyLen) throws IOException {
	createCuckoo.put(valOff, valLen, keyOff, keyLen);
    }
    
    public DataInputBuffer get(DataInputBuffer key) {
	return createCuckoo.get(key);
    }
    
    public MappedByteBuffer getMappedByteBuf() {
	return createCuckoo.dma;
    }

    public int getOffset() {
	return createCuckoo.getAvailableAddress(createCuckoo.dma);
    }

    public void rename() throws IOException {
	createCuckoo.rename();
    }

    public ShmKVIterator getIterator() {
	return lookupCuckoo.getIterator();
    }

    public ShmKVIterator getFinalIterator() {
	return createCuckoo.getIterator();
    }

    public void destroyLookupHash() {
	lookupCuckoo.destroy();
    }

    public void close() {
	createCuckoo.close();
    }

    public long getRawLength() {
	return createCuckoo.getRawLength();
    }
    
    public long getHashSize() {
	return createCuckoo.getHashSize();
    }
    
}

