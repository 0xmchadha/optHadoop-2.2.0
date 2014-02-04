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

package org.apache.hadoop.mapreduce.task;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.BackupStore;
import org.apache.hadoop.mapred.ShmKVIterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;

import org.apache.hadoop.mapred.SharedHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



/**
 * The context passed to the {@link Reducer}.
 * @param <KEYIN> the class of the input keys
 * @param <VALUEIN> the class of the input values
 * @param <KEYOUT> the class of the output keys
 * @param <VALUEOUT> the class of the output values
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ReduceContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    extends TaskInputOutputContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
    implements ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  private ShmKVIterator input;
  private Counter inputValueCounter;
  private Counter inputKeyCounter;
  private RawComparator<KEYIN> comparator;
  private KEYIN key;                                  // current key
  private VALUEIN value;                              // current value
  private VALUEIN value_store;                        // value to be stored in shmfinal
  private boolean firstValue = false;                 // first value in key
  private boolean nextKeyIsSame = false;              // more w/ this key
  private boolean hasMore;                            // more in file
  protected Progressable reporter;
  private Deserializer<KEYIN> keyDeserializer;
  private Deserializer<VALUEIN> valueDeserializer;
  private DataInputBuffer buffer = new DataInputBuffer();
  private BytesWritable currentRawKey = new BytesWritable();
  private ValueIterable iterable = new ValueIterable();
  private boolean isMarked = false;
  private BackupStore<KEYIN,VALUEIN> backupStore;
  private final SerializationFactory serializationFactory;
  private final Class<KEYIN> keyClass;
  private final Class<VALUEIN> valueClass;
  private final Configuration conf;
  private final TaskAttemptID taskid;
  private int currentKeyLength = -1;
  private int currentValueLength = -1;

  private Serializer<VALUEIN> valueSerializer;
  private DataOutputBuffer vbufop = new DataOutputBuffer();
  private DataInputBuffer vbufip = new DataInputBuffer();
  
  //  private SharedHashMap shmFinal;
  
  private static final Log LOG = LogFactory.getLog(ReduceContext.class.getName());
  
  private DataInputBuffer nextKey;
  private DataInputBuffer nextVal;

  public ReduceContextImpl(Configuration conf, TaskAttemptID taskid,
                           ShmKVIterator input, 
                           Counter inputKeyCounter,
                           Counter inputValueCounter,
                           RecordWriter<KEYOUT,VALUEOUT> output,
                           OutputCommitter committer,
                           StatusReporter reporter,
                           Class<KEYIN> keyClass,
                           Class<VALUEIN> valueClass) throws InterruptedException, IOException{
    super(conf, taskid, output, committer, reporter);
    this.input = input;
    this.inputKeyCounter = inputKeyCounter;
    this.inputValueCounter = inputValueCounter;
    this.comparator = comparator;
    this.serializationFactory = new SerializationFactory(conf);
    this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
    this.keyDeserializer.open(buffer);
    this.valueDeserializer = serializationFactory.getDeserializer(valueClass);
    this.valueDeserializer.open(buffer);
    this.valueSerializer = serializationFactory.getSerializer(valueClass);
    this.valueSerializer.open(vbufop);

    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.conf = conf;
    this.taskid = taskid;
  }
  
  public void newIterator() {
      hasMore = input.start();
      nextKeyIsSame = false;
      firstValue = false;
  }
  
  /** Start processing next unique key. */
  public boolean nextKey() throws IOException,InterruptedException {
      while (hasMore && nextKeyIsSame) {
	  nextKeyValue();
      }
      if (hasMore) {
	  if (inputKeyCounter != null) {
	      inputKeyCounter.increment(1);
	  }
	  return nextKeyValue();
      } else {
	  return false;
      }
  }
  
  public DataInputBuffer getKeyBuf() {
      buffer.reset(currentRawKey.getBytes(), 0, currentRawKey.getLength());
      return buffer;
  }
  
  public KEYIN deserializedKey(DataInputBuffer key) throws IOException, InterruptedException {
      buffer.reset(key.getData(), 0, key.getLength());
      return keyDeserializer.deserialize(null);
  }
  
  /**
   * Advance to the next key/value pair.
   */
  @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
      
      if (!hasMore) {
	  key = null;
	  value = null;
	  return false;
      }
      
      firstValue = !nextKeyIsSame;
      
      if (nextKeyIsSame == false) {
	  nextKey = input.getKey();
	  currentRawKey.set(nextKey.getData(), 0,
			    nextKey.getLength());
	  buffer.reset(currentRawKey.getBytes(), 0, currentRawKey.getLength());
	  key = keyDeserializer.deserialize(key);
	  
	  currentKeyLength = nextKey.getLength();
      }
      
      nextVal = input.getValue();
      
      //      LOG.info("val length = " + nextVal.getLength());
      //      for (int i = 0; i < nextVal.getLength(); i++) 
      //  LOG.info(nextVal.getData()[i]);
      
      buffer.reset(nextVal.getData(), 0, nextVal.getLength());

      value = valueDeserializer.deserialize(value);

      currentValueLength = nextVal.getLength();
      
      hasMore = input.next();

      if (hasMore) {
	  nextKeyIsSame = input.isNextKeySame();
      } else {
	  nextKeyIsSame = false;
      }
      inputValueCounter.increment(1);
      return true;
  }
  

  public KEYIN getCurrentKey() {
      return key;
  }
  
  @Override
      public VALUEIN getCurrentValue() {
      return value;
  }
  
  /*  public VALUEIN getStoredVal() throws IOException, InterruptedException {
      buffer.reset(currentRawKey.getBytes(), 0, currentRawKey.getLength());
      DataInputBuffer next = shmFinal.get(buffer);
      if (next != null) {
	  buffer.reset(next.getData(), 0, next.getLength());
	  value_store = valueDeserializer.deserialize(value_store);
	  return value_store;
      }
      return null;
  }
  
  public void store(VALUEIN val) throws IOException, InterruptedException {
      buffer.reset(currentRawKey.getBytes(), 0, currentRawKey.getLength());
      valueSerializer.serialize(val);
      vbufip.reset(vbufop.getData(), vbufop.getLength());
      shmFinal.put(buffer, vbufip); 
      vbufop.reset();
  }
  */
  BackupStore<KEYIN,VALUEIN> getBackupStore() {
      return backupStore;
  }
  
  protected class ValueIterator implements ReduceContext.ValueIterator<VALUEIN> {

    private boolean inReset = false;
    private boolean clearMarkFlag = false;

    @Override
    public boolean hasNext() {
      return firstValue || nextKeyIsSame;
    }

    @Override
    public VALUEIN next() {
      // if this is the first record, we don't need to advance
	if (firstValue) {
	    firstValue = false;
	    return value;
	}
	// if this isn't the first record and the next key is different, they
	// can't advance it here.
	if (!nextKeyIsSame) {
	    throw new NoSuchElementException("iterate past last value");
	}
	
	// otherwise, go to the next key/value pair
      try {
        nextKeyValue();
        return value;
      } catch (IOException ie) {
        throw new RuntimeException("next value iterator failed", ie);
      } catch (InterruptedException ie) {
        // this is bad, but we can't modify the exception list of java.util
        throw new RuntimeException("next value iterator interrupted", ie);        
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove not implemented");
    }

    @Override
    public void mark() throws IOException {
      if (getBackupStore() == null) {
        backupStore = new BackupStore<KEYIN,VALUEIN>(conf, taskid);
      }
      isMarked = true;
      if (!inReset) {
        backupStore.reinitialize();
        if (currentKeyLength == -1) {
          // The user has not called next() for this iterator yet, so
          // there is no current record to mark and copy to backup store.
          return;
        }
        assert (currentValueLength != -1);
        int requestedSize = currentKeyLength + currentValueLength + 
          WritableUtils.getVIntSize(currentKeyLength) +
          WritableUtils.getVIntSize(currentValueLength);
        DataOutputStream out = backupStore.getOutputStream(requestedSize);
        writeFirstKeyValueBytes(out);
        backupStore.updateCounters(requestedSize);
      } else {
        backupStore.mark();
      }
    }

    @Override
    public void reset() throws IOException {
      // We reached the end of an iteration and user calls a 
      // reset, but a clearMark was called before, just throw
      // an exception
      if (clearMarkFlag) {
        clearMarkFlag = false;
        backupStore.clearMark();
        throw new IOException("Reset called without a previous mark");
      }
      
      if (!isMarked) {
        throw new IOException("Reset called without a previous mark");
      }
      inReset = true;
      backupStore.reset();
    }

    @Override
    public void clearMark() throws IOException {
      if (getBackupStore() == null) {
        return;
      }
      if (inReset) {
        clearMarkFlag = true;
        backupStore.clearMark();
      } else {
        inReset = isMarked = false;
        backupStore.reinitialize();
      }
    }
    
    /**
     * This method is called when the reducer moves from one key to 
     * another.
     * @throws IOException
     */
    public void resetBackupStore() throws IOException {
      if (getBackupStore() == null) {
        return;
      }
      inReset = isMarked = false;
      backupStore.reinitialize();
      currentKeyLength = -1;
    }

    /**
     * This method is called to write the record that was most recently
     * served (before a call to the mark). Since the framework reads one
     * record in advance, to get this record, we serialize the current key
     * and value
     * @param out
     * @throws IOException
     */
    private void writeFirstKeyValueBytes(DataOutputStream out) 
    throws IOException {
      assert (getCurrentKey() != null && getCurrentValue() != null);
      WritableUtils.writeVInt(out, currentKeyLength);
      WritableUtils.writeVInt(out, currentValueLength);
      Serializer<KEYIN> keySerializer = 
        serializationFactory.getSerializer(keyClass);
      keySerializer.open(out);
      keySerializer.serialize(getCurrentKey());

      Serializer<VALUEIN> valueSerializer = 
        serializationFactory.getSerializer(valueClass);
      valueSerializer.open(out);
      valueSerializer.serialize(getCurrentValue());
    }
  }

  protected class ValueIterable implements Iterable<VALUEIN> {
    private ValueIterator iterator = new ValueIterator();
    @Override
    public Iterator<VALUEIN> iterator() {
      return iterator;
    } 
  }
  
  /**
   * Iterate through the values for the current key, reusing the same value 
   * object, which is stored in the context.
   * @return the series of values associated with the current key. All of the 
   * objects returned directly and indirectly from this method are reused.
   */
  public 
  Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
    return iterable;
  }
}
