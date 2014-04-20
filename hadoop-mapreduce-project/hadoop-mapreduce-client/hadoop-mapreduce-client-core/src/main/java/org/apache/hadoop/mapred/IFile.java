 /**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
 package org.apache.hadoop.mapred;

 import java.io.DataInput;
 import java.io.DataInputStream;
 import java.io.DataOutputStream;
 import java.io.EOFException;
 import java.io.IOException;
 import java.io.InputStream;
 import java.nio.MappedByteBuffer;
 import java.util.ArrayList;
 import java.util.Iterator;

 import org.apache.hadoop.classification.InterfaceAudience;
 import org.apache.hadoop.classification.InterfaceStability;
 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.FSDataInputStream;
 import org.apache.hadoop.fs.FSDataOutputStream;
 import org.apache.hadoop.fs.FileSystem;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.DataInputBuffer;
 import org.apache.hadoop.io.DataOutputBuffer;
 import org.apache.hadoop.io.MappedDataOutputBuffer;
 import org.apache.hadoop.io.IOUtils;
 import org.apache.hadoop.io.WritableUtils;
 import org.apache.hadoop.io.compress.CodecPool;
 import org.apache.hadoop.io.compress.CompressionCodec;
 import org.apache.hadoop.io.compress.CompressionOutputStream;
 import org.apache.hadoop.io.compress.Compressor;
 import org.apache.hadoop.io.compress.Decompressor;
 import org.apache.hadoop.io.serializer.SerializationFactory;
 import org.apache.hadoop.io.serializer.Serializer;

 import org.apache.hadoop.mapred.SharedHashCreate;
 import org.apache.hadoop.mapreduce.Counter;
 import org.apache.hadoop.mapreduce.OutputCommitter;
 import org.apache.hadoop.mapreduce.RecordWriter;
 import org.apache.hadoop.mapreduce.ReduceContext;
 import org.apache.hadoop.mapreduce.StatusReporter;
 import org.apache.hadoop.mapreduce.TaskAttemptID;
 import org.apache.hadoop.util.Progressable;

 import org.apache.hadoop.mapreduce.task.TaskInputOutputContextImpl;
 import org.apache.commons.logging.Log;
 import org.apache.commons.logging.LogFactory;
 import org.apache.hadoop.mapred.SharedHashLookup.shmList;
 /**
  * <code>IFile</code> is the simple <key-len, value-len, key, value> format
  * for the intermediate map-outputs in Map-Reduce.
  *
  * There is a <code>Writer</code> to write out map-outputs in this format and 
  * a <code>Reader</code> to read files of this format.
  */
 @InterfaceAudience.Private
     @InterfaceStability.Unstable
     public class IFile {
	 private static final Log LOG = LogFactory.getLog(IFile.class);
	 public static final int EOF_MARKER = -1; // End of File Marker

	 public static class shmWriter<K extends Object, V extends Object> {


	     public class kvHolder
	     {
		 ArrayList<K> kA;
		 ArrayList<ArrayList<V>> vA;
		 int arrIndex;
	     }

	     Class<K> keyClass;
	     Class<V> valueClass;
	     Serializer<K> keySerializer;
	     Serializer<K> keySerializer2;
	     Serializer<V> valueSerializer;
	     ArrayList<kvHolder> hold;
		 //	    ArrayList<ArrayList<K>> kA;
		 //	    ArrayList<ArrayList<V>> vA;

	     public class DeserializedContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
		 extends TaskInputOutputContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT> implements ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
		 private Counter inputValueCounter;
		 private Counter inputKeyCounter;
		 private ValueIterable iterable = new ValueIterable();
		 private int partNum;
		 private int i;

		 public DeserializedContext(Configuration conf, TaskAttemptID taskid, RecordWriter<KEYOUT, VALUEOUT> output, OutputCommitter committer, StatusReporter reporter, Counter inputKeyCounter, Counter inputValueCounter) {
		     super(conf, taskid, output, committer, reporter);

		     this.inputKeyCounter = inputKeyCounter;
		     this.inputValueCounter = inputValueCounter;
		     i = -1;
		 }

		 public boolean nextKeyValue() {
		     return false;
		 }

		 public void setShl(SharedHashLookup shl, ArrayList<shmList> shmlist) {
		 }

		 public void setCombiner() {
		 }

		 public DataInputBuffer getKeyBuf() {
		     return null;
		 }

		 public KEYIN deserializedKey(DataInputBuffer key) throws IOException, InterruptedException {
		     return null;
		 }
		 
		 public boolean nextKey() throws IOException, InterruptedException {

		      if (partNum == hold.size())
			  return false;

		      i++;

		      if (i == 0) 
			  setHashMap(partNum);
		      
		      if (hold.get(partNum).kA.size() == i) {
			  partNum++;
			  i = -1;
			  return nextKey();
		      }
		      
		      return true;
		 }
	     
		public void setKey() {
		}

		public KEYIN getCurrentKey() {
		    return (KEYIN)hold.get(partNum).kA.get(i);
		}
		
		public void newIterator() {
		}

		public void newIterator(int num) {
		}

		public VALUEIN getCurrentValue() {
		    return null;
		}
		
		protected class ValueIterator implements Iterator<VALUEIN> {
		    int j;
		    
		    public ValueIterator() {
			j = -1;
		    }
		    
		    public boolean hasNext() {
			j++;
			
			if (j == hold.get(partNum).vA.get(i).size()) {
			    j = -1;
			    return false;
			}
			
			return true;
		    }
		 
		    public VALUEIN next() {
			return (VALUEIN)hold.get(partNum).vA.get(i).get(j);
		    }

		    public void remove() {
		    }
		}
	     
		protected class ValueIterable implements Iterable<VALUEIN> {
		    private ValueIterator iterator = new ValueIterator();
		    @Override
			public Iterator<VALUEIN> iterator() {
			return iterator;
		    }
		}
	     
		public Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
		    return iterable;
		}
	    }
	 
	    private long numRecordsWritten = 0;
	    private Counters.Counter writtenRecordsCounter;
	    MappedDataOutputBuffer kvbuf;
	    DataOutputBuffer kvbuf2;
	    private SharedHashCreate shms;
	    private int pred_uniq_keys;
	    DataInputBuffer dib;

	    public shmWriter(Configuration conf, 
			     int partitions, 
			     Class<K> keyClass,
			     Class<V> valueClass,
			     CompressionCodec codec,
			     Counters.Counter writesCounter) throws IOException {
		int bs, rs, rts;
		int check;
		float avg_col;
		JobConf job = (JobConf)conf;
		kvHolder h;
		
		this.keyClass = keyClass;
		this.valueClass = valueClass;
		SerializationFactory serializationFactory = new SerializationFactory(conf);
		this.writtenRecordsCounter = writesCounter;
		this.keySerializer = serializationFactory.getSerializer(keyClass);
		this.keySerializer2 = serializationFactory.getSerializer(keyClass);
		kvbuf2 = new DataOutputBuffer();
		kvbuf = new MappedDataOutputBuffer();
		dib = new DataInputBuffer();
		//		kA = new ArrayList<ArrayList<K>>();
		//		vA = new ArrayList<ArrayList<V>>();
		hold = new ArrayList<kvHolder>(partitions);

		for (int i = 0; i < partitions; i++) {
		    h = hold.get(i);
		    h.kA = new ArrayList<K>();
		    h.vA = new ArrayList<ArrayList<V>>();
		}

		this.keySerializer.open(kvbuf);
		this.keySerializer2.open(kvbuf2);
		this.valueSerializer = serializationFactory.getSerializer(valueClass);
		this.valueSerializer.open(kvbuf);
	 
		check = Integer.parseInt(job.get("CHANGE_DEFAULT"));

		if (check == 1) {
		    bs = Integer.parseInt(job.get("BLOCK_SIZE"));
		    rs = Integer.parseInt(job.get("AVG_REC_SIZE"));
		    rts = job.getNumReduceTasks();
		    avg_col = Float.parseFloat(job.get("AVG_COLLISION"));
	     
		    pred_uniq_keys = (int)((bs/(rs*rts*avg_col)) * 100/93);
		} else 
		    pred_uniq_keys = 65536/4;
             
		shms = new SharedHashCreate(partitions);
	    }
	    
	    public void close_static() throws IOException {
		// Close the serializers
		keySerializer.close();
		keySerializer2.close();
		valueSerializer.close();
		
		if(writtenRecordsCounter != null) {
		    writtenRecordsCounter.increment(numRecordsWritten);
		}
	    }
    
	    public int newReducer(int num_reducer, Path shmFile, 
				  int hash_size, int keys) throws IOException {
		if (num_reducer == -1) {
		    if (keys == 0)
			return shms.setHashMap(shmFile.toString(), hash_size, pred_uniq_keys);
		    else
			return shms.setHashMap(shmFile.toString(), hash_size, keys);
		}
             
		if (keys == 0) {
		    shms.setHashMap(num_reducer, shmFile.toString(), hash_size, pred_uniq_keys);
		} else {
		    shms.setHashMap(num_reducer, shmFile.toString(), hash_size, keys);
		}
	     
		return num_reducer;
	    }

	    public void close(int num_reducer) throws IOException {
		shms.close(num_reducer);
	    }
	 
	    private int getIndex(DataInputBuffer input) {
		int index;
		if (input == null)
		    return -1;
		byte[] result = input.getData();
	     
		index = (result[3]<<24)&0xff000000|(result[2]<<16)&0xff0000|(result[1]<<8)&0xff00|(result[0]<<0)&0xff;
		//	    index = (result[3] << 24 | result[2] << 16 | result[1] << 8 | result[0]);
	     
		return index;
	    }
	 
	    public int writeShm(MappedByteBuffer mbf, int off, int i) throws IOException {
		byte mc_b = WritableUtils.writeIntOpt(4);
	    
		mbf.put(off, mc_b);
		mbf.put(off + 1, (byte) (i));
		mbf.put(off + 2, (byte) (i >> 8));
		mbf.put(off + 3, (byte) (i >> 16));
		mbf.put(off + 4, (byte) (i >> 24)); 
		return off+1;
	    }

	    public int writeShm(MappedByteBuffer mbf, int off, DataInputBuffer key) throws IOException {
		byte[] keyb = key.getData();
		int len = key.getLength();

		byte mc_b = WritableUtils.writeIntOpt(len);
		mbf.put(off, mc_b);
	    
		for (int i = 0; i < len; i++) {
		    mbf.put(off + 1 + i, keyb[i]);
		}

		return off+1;
	    }
	 
	    public void append(K key, V value, int keyPart, int hashPart ) throws IOException {
		MappedByteBuffer mbf;
		int offset, index;
		int keyOff, valOff;

		if (key.getClass() != keyClass)
		    throw new IOException("wrong key class: "+ key.getClass()
					  +" is not "+ keyClass);
		if (value.getClass() != valueClass)
		    throw new IOException("wrong value class: "+ value.getClass()
					  +" is not "+ valueClass);
	     
		mbf = shms.getMappedByteBuf(hashPart);
		offset = shms.getOffset(hashPart);
	     
		keySerializer2.serialize(key);
		dib.reset(kvbuf2.getData(), 0, kvbuf2.getLength());
		
		index = getIndex(shms.get(hashPart, dib));
	     
		if (index == -1) {
		    //   compKey c = new compKey(key, keyPart);
		    int arrIndex = hold.get(keyPart).arrIndex;
		    hold.get(keyPart).kA.add(arrIndex, key);
		    //		    kA.get(keyPart).add(arrIndex, c);
		    offset = shms.getOffset(hashPart);
		    valOff = writeShm(mbf, offset, arrIndex);
		    keyOff = writeShm(mbf, offset + 9, dib);
		    shms.put(hashPart, valOff, 4, keyOff, dib.getLength());
		    index = arrIndex;
		    ArrayList<V> v = new ArrayList();
		    hold.get(keyPart).vA.add(index, v);
		    hold.get(keyPart).arrIndex++;
		}

		hold.get(keyPart).vA.get(index).add(value);
		kvbuf2.reset();
	    }

	    public void append(K key, V value, int num_reducer) throws IOException {
		int get_offset;
		int bytes_written;
		MappedByteBuffer mbf;
		int valOff, valLen, keyOff, keyLen;
	
		if (key.getClass() != keyClass)
		    throw new IOException("wrong key class: "+ key.getClass()
					  +" is not "+ keyClass);
		if (value.getClass() != valueClass)
		    throw new IOException("wrong value class: "+ value.getClass()
					  +" is not "+ valueClass);
	    
		mbf = shms.getMappedByteBuf(num_reducer);
		get_offset = shms.getOffset(num_reducer);
		// Append the 'key' and 'val'	    	    
	    
		kvbuf.change_buf(mbf, get_offset + 1);
		valOff = get_offset + 1;
		valueSerializer.serialize(value);
		bytes_written = kvbuf.numWrittenBytes();
		valLen = bytes_written;

		if (bytes_written >= 127 || bytes_written < 0) {
		    // error
		    throw new IOException("key value size not in limit");
		}

		byte mc_b;
		mc_b = WritableUtils.writeIntOpt(bytes_written);
		mbf.put(get_offset, mc_b);
		get_offset += (1 + bytes_written);
	    
		/* write null pointer */
		//	    for(int i = 0; i < 4; i++) 
		mbf.putInt(get_offset, 0);

		get_offset += (1 + 4);
		kvbuf.change_buf(mbf, get_offset);
		keyOff = get_offset;
		keySerializer.serialize(key);
		bytes_written = kvbuf.numWrittenBytes();
		keyLen = bytes_written;
	     
		if (bytes_written >= 127)  {
		    return;
		    //		throw new IOException("key value size not in limit");
		    // error
		}
	    
		mc_b = WritableUtils.writeIntOpt(bytes_written);
		mbf.put(get_offset - 1, mc_b);
	    	    
		/*	    int keyLength = kbuf.getLength();
			    if (keyLength < 0 || bytes_written < 0) {
			    throw new IOException("Negative key-length not allowed: " + keyLength + 
			    " for " + key);
			    }
		    
			    // Append the 'value'
	    
			    int valueLength = vbuf.getLength();
			    if (valueLength < 0) {
			    throw new IOException("Negative value-length not allowed: " + 
			    valueLength + " for " + value);
			    }
		*/
		// Write to sharedhashmap
		//	    kinput.reset(kbuf.getData(), kbuf.getLength());
		//	    vinput.reset(vbuf.getData(), vbuf.getLength());
		shms.put(num_reducer, valOff, valLen, keyOff, keyLen);
		// Reset
		kvbuf.reset();
		// Update bytes written
		//	    decompressedBytesWritten += keyLength + valueLength + 8; // currently 4 bytes each are being used to write keylen and val len

		//		WritableUtils.getVIntSize(keyLength) + 
		//	WritableUtils.getVIntSize(valueLength);
		++numRecordsWritten;
	    }
	
	    public void rename(int num_reducer) throws IOException {
		shms.rename(num_reducer);
	    }

	    public long getRawLength(int num_reducer) {
		return shms.getRawLength(num_reducer);
	    }
	
	    public long getCompressedLength(int num_reducer) {
		return shms.getRawLength(num_reducer);
	    }
      
	    public long getHashSize(int num_reducer) {
		return shms.getHashSize(num_reducer);
	    }
	
	    public ShmKVIterator getIterator() {
		return shms.getIterator();
	    }
         
	    public void setIterator(int hashmap_num) {
		shms.setIterator(hashmap_num);
	    }
         
	    public void replaceWriter(int i, int j) {
		shms.replaceWriter(i, j);
	    }
	}
     
	/**
	 * <code>IFile.Writer</code> to write out intermediate map-outputs. 
	 */
	@InterfaceAudience.Private
	    @InterfaceStability.Unstable
	    public static class Writer<K extends Object, V extends Object> {
	    FSDataOutputStream out;
	    boolean ownOutputStream = false;
	    long start = 0;
	    FSDataOutputStream rawOut;
    
	    CompressionOutputStream compressedOut;
	    Compressor compressor;
	    boolean compressOutput = false;
    
	    long decompressedBytesWritten = 0;
	    long compressedBytesWritten = 0;

	    // Count records written to disk
	    private long numRecordsWritten = 0;
	    private final Counters.Counter writtenRecordsCounter;

	    IFileOutputStream checksumOut;

	    Class<K> keyClass;
	    Class<V> valueClass;
	    Serializer<K> keySerializer;
	    Serializer<V> valueSerializer;
    
	    DataOutputBuffer buffer = new DataOutputBuffer();

	    public Writer(Configuration conf, FileSystem fs, Path file, 
			  Class<K> keyClass, Class<V> valueClass,
			  CompressionCodec codec,
			  Counters.Counter writesCounter) throws IOException {
		this(conf, fs.create(file), keyClass, valueClass, codec,
		     writesCounter);
		ownOutputStream = true;
	    }
    
	    protected Writer(Counters.Counter writesCounter) {
		writtenRecordsCounter = writesCounter;
	    }

	    public Writer(Configuration conf, FSDataOutputStream out, 
			  Class<K> keyClass, Class<V> valueClass,
			  CompressionCodec codec, Counters.Counter writesCounter)
		throws IOException {
		this.writtenRecordsCounter = writesCounter;
		this.checksumOut = new IFileOutputStream(out);
		this.rawOut = out;
		this.start = this.rawOut.getPos();
		if (codec != null) {
		    this.compressor = CodecPool.getCompressor(codec);
		    if (this.compressor != null) {
			this.compressor.reset();
			this.compressedOut = codec.createOutputStream(checksumOut, compressor);
			this.out = new FSDataOutputStream(this.compressedOut,  null);
			this.compressOutput = true;
		    } else {
			LOG.warn("Could not obtain compressor from CodecPool");
			this.out = new FSDataOutputStream(checksumOut,null);
		    }
		} else {
		    this.out = new FSDataOutputStream(checksumOut,null);
		}
      
		this.keyClass = keyClass;
		this.valueClass = valueClass;

		if (keyClass != null) {
		    SerializationFactory serializationFactory = 
			new SerializationFactory(conf);
		    this.keySerializer = serializationFactory.getSerializer(keyClass);
		    this.keySerializer.open(buffer);
		    this.valueSerializer = serializationFactory.getSerializer(valueClass);
		    this.valueSerializer.open(buffer);
		}
	    }

	    public Writer(Configuration conf, FileSystem fs, Path file) 
		throws IOException {
		this(conf, fs, file, null, null, null, null);
	    }

	    public void close() throws IOException {

		// When IFile writer is created by BackupStore, we do not have
		// Key and Value classes set. So, check before closing the
		// serializers
		if (keyClass != null) {
		    keySerializer.close();
		    valueSerializer.close();
		}

		// Write EOF_MARKER for key/value length
		WritableUtils.writeVInt(out, EOF_MARKER);
		WritableUtils.writeVInt(out, EOF_MARKER);
		decompressedBytesWritten += 2 * WritableUtils.getVIntSize(EOF_MARKER);
      
		//Flush the stream
		out.flush();
  
		if (compressOutput) {
		    // Flush
		    compressedOut.finish();
		    compressedOut.resetState();
		}
      
		// Close the underlying stream iff we own it...
		if (ownOutputStream) {
		    out.close();
		}
		else {
		    // Write the checksum
		    checksumOut.finish();
		}

		compressedBytesWritten = rawOut.getPos() - start;

		if (compressOutput) {
		    // Return back the compressor
		    CodecPool.returnCompressor(compressor);
		    compressor = null;
		}

		out = null;
		if(writtenRecordsCounter != null) {
		    writtenRecordsCounter.increment(numRecordsWritten);
		}
	    }

	    public void append(K key, V value) throws IOException {
		if (key.getClass() != keyClass)
		    throw new IOException("wrong key class: "+ key.getClass()
					  +" is not "+ keyClass);
		if (value.getClass() != valueClass)
		    throw new IOException("wrong value class: "+ value.getClass()
					  +" is not "+ valueClass);

		// Append the 'key'
		keySerializer.serialize(key);
		int keyLength = buffer.getLength();
		if (keyLength < 0) {
		    throw new IOException("Negative key-length not allowed: " + keyLength + 
					  " for " + key);
		}

		// Append the 'value'
		valueSerializer.serialize(value);
		int valueLength = buffer.getLength() - keyLength;
		if (valueLength < 0) {
		    throw new IOException("Negative value-length not allowed: " + 
					  valueLength + " for " + value);
		}
      
		// Write the record out
		WritableUtils.writeVInt(out, keyLength);                  // key length
		WritableUtils.writeVInt(out, valueLength);                // value length
		out.write(buffer.getData(), 0, buffer.getLength());       // data

		// Reset
		buffer.reset();
      
		// Update bytes written
		decompressedBytesWritten += keyLength + valueLength + 
		    WritableUtils.getVIntSize(keyLength) + 
		    WritableUtils.getVIntSize(valueLength);
		++numRecordsWritten;
	    }
    
	    public void append(DataInputBuffer key, DataInputBuffer value)
		throws IOException {
		int keyLength = key.getLength() - key.getPosition();
		if (keyLength < 0) {
		    throw new IOException("Negative key-length not allowed: " + keyLength + 
					  " for " + key);
		}
      
		int valueLength = value.getLength() - value.getPosition();
		if (valueLength < 0) {
		    throw new IOException("Negative value-length not allowed: " + 
					  valueLength + " for " + value);
		}

		WritableUtils.writeVInt(out, keyLength);
		WritableUtils.writeVInt(out, valueLength);
		out.write(key.getData(), key.getPosition(), keyLength); 
		out.write(value.getData(), value.getPosition(), valueLength); 

		// Update bytes written
		decompressedBytesWritten += keyLength + valueLength + 
		    WritableUtils.getVIntSize(keyLength) + 
		    WritableUtils.getVIntSize(valueLength);
		++numRecordsWritten;
	    }
    
	    // Required for mark/reset
	    public DataOutputStream getOutputStream () {
		return out;
	    }
    
	    // Required for mark/reset
	    public void updateCountersForExternalAppend(long length) {
		++numRecordsWritten;
		decompressedBytesWritten += length;
	    }
    
	    public long getRawLength() {
		return decompressedBytesWritten;
	    }
    
	    public long getCompressedLength() {
		return compressedBytesWritten;
	    }
	}

	/**
	 * <code>IFile.Reader</code> to read intermediate map-outputs. 
	 */
	@InterfaceAudience.Private
	    @InterfaceStability.Unstable
	    public static class Reader<K extends Object, V extends Object> {
	    private static final int DEFAULT_BUFFER_SIZE = 128*1024;
	    private static final int MAX_VINT_SIZE = 9;

	    // Count records read from disk
	    private long numRecordsRead = 0;
	    private final Counters.Counter readRecordsCounter;

	    final InputStream in;        // Possibly decompressed stream that we read
	    Decompressor decompressor;
	    public long bytesRead = 0;
	    protected final long fileLength;
	    protected boolean eof = false;
	    final IFileInputStream checksumIn;
    
	    protected byte[] buffer = null;
	    protected int bufferSize = DEFAULT_BUFFER_SIZE;
	    protected DataInputStream dataIn;

	    protected int recNo = 1;
	    protected int currentKeyLength;
	    protected int currentValueLength;
	    byte keyBytes[] = new byte[0];
    
    
	    /**
	     * Construct an IFile Reader.
	     * 
	     * @param conf Configuration File 
	     * @param fs  FileSystem
	     * @param file Path of the file to be opened. This file should have
	     *             checksum bytes for the data at the end of the file.
	     * @param codec codec
	     * @param readsCounter Counter for records read from disk
	     * @throws IOException
	     */
	    public Reader(Configuration conf, FileSystem fs, Path file,
			  CompressionCodec codec,
			  Counters.Counter readsCounter) throws IOException {
		this(conf, fs.open(file), 
		     fs.getFileStatus(file).getLen(),
		     codec, readsCounter);
	    }

	    /**
	     * Construct an IFile Reader.
	     * 
	     * @param conf Configuration File 
	     * @param in   The input stream
	     * @param length Length of the data in the stream, including the checksum
	     *               bytes.
	     * @param codec codec
	     * @param readsCounter Counter for records read from disk
	     * @throws IOException
	     */
	    public Reader(Configuration conf, FSDataInputStream in, long length, 
			  CompressionCodec codec,
			  Counters.Counter readsCounter) throws IOException {
		readRecordsCounter = readsCounter;
		checksumIn = new IFileInputStream(in,length, conf);
		if (codec != null) {
		    decompressor = CodecPool.getDecompressor(codec);
		    if (decompressor != null) {
			this.in = codec.createInputStream(checksumIn, decompressor);
		    } else {
			LOG.warn("Could not obtain decompressor from CodecPool");
			this.in = checksumIn;
		    }
		} else {
		    this.in = checksumIn;
		}
		this.dataIn = new DataInputStream(this.in);
		this.fileLength = length;
      
		if (conf != null) {
		    bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
		}
	    }
    
	    public long getLength() { 
		return fileLength - checksumIn.getSize();
	    }
    
	    public long getPosition() throws IOException {    
		return checksumIn.getPosition(); 
	    }
    
	    /**
	     * Read upto len bytes into buf starting at offset off.
	     * 
	     * @param buf buffer 
	     * @param off offset
	     * @param len length of buffer
	     * @return the no. of bytes read
	     * @throws IOException
	     */
	    private int readData(byte[] buf, int off, int len) throws IOException {
		int bytesRead = 0;
		while (bytesRead < len) {
		    int n = IOUtils.wrappedReadForCompressedData(in, buf, off + bytesRead,
								 len - bytesRead);
		    if (n < 0) {
			return bytesRead;
		    }
		    bytesRead += n;
		}
		return len;
	    }
    
	    protected boolean positionToNextRecord(DataInput dIn) throws IOException {
		// Sanity check
		if (eof) {
		    throw new EOFException("Completed reading " + bytesRead);
		}
      
		// Read key and value lengths
		currentKeyLength = WritableUtils.readVInt(dIn);
		currentValueLength = WritableUtils.readVInt(dIn);
		bytesRead += WritableUtils.getVIntSize(currentKeyLength) +
		    WritableUtils.getVIntSize(currentValueLength);
      
		// Check for EOF
		if (currentKeyLength == EOF_MARKER && currentValueLength == EOF_MARKER) {
		    eof = true;
		    return false;
		}
      
		// Sanity check
		if (currentKeyLength < 0) {
		    throw new IOException("Rec# " + recNo + ": Negative key-length: " + 
					  currentKeyLength);
		}
		if (currentValueLength < 0) {
		    throw new IOException("Rec# " + recNo + ": Negative value-length: " + 
					  currentValueLength);
		}
            
		return true;
	    }
    
	    public boolean nextRawKey(DataInputBuffer key) throws IOException {
		if (!positionToNextRecord(dataIn)) {
		    return false;
		}
		if (keyBytes.length < currentKeyLength) {
		    keyBytes = new byte[currentKeyLength << 1];
		}
		int i = readData(keyBytes, 0, currentKeyLength);
		if (i != currentKeyLength) {
		    throw new IOException ("Asked for " + currentKeyLength + " Got: " + i);
		}
		key.reset(keyBytes, currentKeyLength);
		bytesRead += currentKeyLength;
		return true;
	    }
    
	    public void nextRawValue(DataInputBuffer value) throws IOException {
		final byte[] valBytes = (value.getData().length < currentValueLength)
		    ? new byte[currentValueLength << 1]
		    : value.getData();
		int i = readData(valBytes, 0, currentValueLength);
		if (i != currentValueLength) {
		    throw new IOException ("Asked for " + currentValueLength + " Got: " + i);
		}
		value.reset(valBytes, currentValueLength);
      
		// Record the bytes read
		bytesRead += currentValueLength;

		++recNo;
		++numRecordsRead;
	    }
    
	    public void close() throws IOException {
		// Close the underlying stream
		in.close();
      
		// Release the buffer
		dataIn = null;
		buffer = null;
		if(readRecordsCounter != null) {
		    readRecordsCounter.increment(numRecordsRead);
		}

		// Return the decompressor
		if (decompressor != null) {
		    decompressor.reset();
		    CodecPool.returnDecompressor(decompressor);
		    decompressor = null;
		}
	    }
    
	    public void reset(int offset) {
		return;
	    }

	    public void disableChecksumValidation() {
		checksumIn.disableChecksumValidation();
	    }

	}    
    }
