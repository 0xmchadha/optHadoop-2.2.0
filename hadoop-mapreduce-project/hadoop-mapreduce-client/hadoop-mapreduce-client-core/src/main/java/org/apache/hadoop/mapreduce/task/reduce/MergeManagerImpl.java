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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.io.RandomAccessFile;

import java.lang.String;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.Merger;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.Task.CombineOutputCollector;
import org.apache.hadoop.mapred.Task.CombineValuesIterator;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.task.reduce.MapOutput.MapOutputComparator;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.mapred.SharedHashLookup;
import org.apache.hadoop.mapred.SharedHashLookup.shmList;
import com.google.common.annotations.VisibleForTesting;

@SuppressWarnings(value={"unchecked"})
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class MergeManagerImpl<K, V> implements MergeManager<K, V> {
  
  private static final Log LOG = LogFactory.getLog(MergeManagerImpl.class);
  
  /* Maximum percentage of the in-memory limit that a single shuffle can 
   * consume*/ 
  private static final float DEFAULT_SHUFFLE_MEMORY_LIMIT_PERCENT
    = 0.25f;

  private final TaskAttemptID reduceId;
  
  private final JobConf jobConf;
  private final FileSystem localFS;
  private final FileSystem rfs;
  private final LocalDirAllocator localDirAllocator;
  
  protected MapOutputFile mapOutputFile;
  
  private final shmRun shmrun;

  private final Reporter reporter;
  private final ExceptionReporter exceptionReporter;
  
  /**
   * Combiner class to run during in-memory merge, if defined.
   */
  private final Class<? extends Reducer> combinerClass;

  /**
   * Resettable collector used for combine.
   */
  private final CombineOutputCollector<K,V> combineCollector;

  private final Counters.Counter spilledRecordsCounter;

  private final Counters.Counter reduceCombineInputCounter;

  private final Counters.Counter mergedMapOutputsCounter;
  
  private final CompressionCodec codec;
  
  private final Progress mergePhase;
  
  private final SharedHashLookup shLookups;

  private Task reduceTask;
  
  private final ArrayList<shmList> shmlist = new ArrayList<shmList>();

  public MergeManagerImpl(TaskAttemptID reduceId, Task reduceTask,
			  JobConf jobConf, 
			  FileSystem localFS,
			  LocalDirAllocator localDirAllocator,  
			  Reporter reporter,
			  CompressionCodec codec,
			  Class<? extends Reducer> combinerClass,
			  CombineOutputCollector<K,V> combineCollector,
			  Counters.Counter spilledRecordsCounter,
			  Counters.Counter reduceCombineInputCounter,
			  Counters.Counter mergedMapOutputsCounter,
			  ExceptionReporter exceptionReporter,
			  Progress mergePhase, MapOutputFile mapOutputFile,
                          SharedHashLookup shLookups) {
    this.reduceId = reduceId;
    this.jobConf = jobConf;
    this.localDirAllocator = localDirAllocator;
    this.exceptionReporter = exceptionReporter;
    
    this.reporter = reporter;
    this.codec = codec;
    this.combinerClass = combinerClass;
    this.combineCollector = combineCollector;
    this.reduceCombineInputCounter = reduceCombineInputCounter;
    this.spilledRecordsCounter = spilledRecordsCounter;
    this.mergedMapOutputsCounter = mergedMapOutputsCounter;
    this.mapOutputFile = mapOutputFile;
    this.mapOutputFile.setConf(jobConf);
    
    this.localFS = localFS;
    this.rfs = ((LocalFileSystem)localFS).getRaw();

    this.shLookups = shLookups;
    this.reduceTask = reduceTask;
    this.shmrun = new shmRun(this);
    this.shmrun.start();

    this.mergePhase = mergePhase;
  }
  
  TaskAttemptID getReduceId() {
    return reduceId;
  }

  @VisibleForTesting
  ExceptionReporter getExceptionReporter() {
    return exceptionReporter;
  }
  
  @Override
      public void waitForResource() throws InterruptedException {
  }
  
  @Override
      public MapOutput<K,V> reserve(TaskAttemptID mapId, 
				    long requestedSize,
				    int fetcher
				    ) throws IOException {
      return new OnDiskMapOutput<K,V>(mapId, reduceId, this, requestedSize,
                                      jobConf, mapOutputFile, fetcher, true);
  }
  
  void unreserve(long size) {
	  
  }
  
  public void closeOnDiskFile(CompressAwarePath file) {
      shmList tmp;
      RandomAccessFile shf;
      MappedByteBuffer shm = null;
      String fileName = file.toString();

      shmrun.numPending.incrementAndGet();
      try {
	  shf = new RandomAccessFile(fileName, "r");
	  shm = shf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, shf.length());
	  shm.load(); 
      } catch (IOException ie) {
	  
      }

      tmp = new shmList();
      tmp.file = fileName;
      tmp.shm = shm;
      
      synchronized(shmlist) {
	  shmlist.add(tmp);
	  shmlist.notify();
      }
  }
  
  @Override
  public RawKeyValueIterator close() throws Throwable {
      shmrun.close();
      return null;
  }
   
  private class shmRun extends MergeThread<SharedHashLookup, K, V> {
      public shmRun(MergeManagerImpl <K, V> manager) {
	  super(manager, 0, exceptionReporter);
	  setName("shmRun");
	  setDaemon(true);
      }
      
      public void merge(List<SharedHashLookup> inputs) throws IOException {
	  
      }
      
      public void closeAll() {
	  synchronized(shmlist) {
	      shmlist.notify();
	  }
      }

      public void run() {
          ((org.apache.hadoop.mapred.ReduceTask)reduceTask).iterate.startProcessing(shmlist);	

	  while (true) {
	      try {
		  // Wait for notification to start the merge...
		  synchronized (shmlist) {
		      while(shmlist.size() <= 0 && closed == false) {
			  shmlist.wait();
		      }
		  }

		  if (closed == true) {
		      numPending.set(0);
		      return;
		  }
		  
		  while (shmlist.size() > 0) {
		      shmList Shm;
		      synchronized (shmlist) {
			  Shm = shmlist.get(0);
			  shmlist.remove(0);
		      }
                      shLookups.setNewLookup(Shm.file, Shm.shm);
		      ((org.apache.hadoop.mapred.ReduceTask)reduceTask).iterate.startProcessing();	
		      // system.gc();
		  }
	      } catch (InterruptedException ie) {
		  numPending.set(0);
		  return;
	      } catch(Throwable t) {
		  numPending.set(0);
		  reporterExcep.reportException(t);
		  return;
	      } finally {
		  synchronized (numPending) {
		      numPending.decrementAndGet();
		      numPending.notifyAll();
		  }
	      }
	  }
      }
  }


  class RawKVIteratorReader extends IFile.Reader<K,V> {

    private final RawKeyValueIterator kvIter;

    public RawKVIteratorReader(RawKeyValueIterator kvIter, long size)
        throws IOException {
      super(null, null, size, null, spilledRecordsCounter);
      this.kvIter = kvIter;
    }
    public boolean nextRawKey(DataInputBuffer key) throws IOException {
      if (kvIter.next()) {
        final DataInputBuffer kb = kvIter.getKey();
        final int kp = kb.getPosition();
        final int klen = kb.getLength() - kp;
        key.reset(kb.getData(), kp, klen);
        bytesRead += klen;
        return true;
      }
      return false;
    }

    public void nextRawValue(DataInputBuffer value) throws IOException {
      final DataInputBuffer vb = kvIter.getValue();
      final int vp = vb.getPosition();
      final int vlen = vb.getLength() - vp;
      value.reset(vb.getData(), vp, vlen);
      bytesRead += vlen;
    }

    public long getPosition() throws IOException {
      return bytesRead;
    }

    public void close() throws IOException {
      kvIter.close();
    }
  }

  static class CompressAwarePath extends Path {
    private long rawDataLength;
    private long compressedSize;

    public CompressAwarePath(Path path, long rawDataLength, long compressSize) {
      super(path.toUri());
      this.rawDataLength = rawDataLength;
      this.compressedSize = compressSize;
    }

    public long getRawDataLength() {
      return rawDataLength;
    }

    public long getCompressedSize() {
      return compressedSize;
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public int compareTo(Object obj) {
      if(obj instanceof CompressAwarePath) {
        CompressAwarePath compPath = (CompressAwarePath) obj;
        if(this.compressedSize < compPath.getCompressedSize()) {
          return -1;
        } else if (this.getCompressedSize() > compPath.getCompressedSize()) {
          return 1;
        }
        // Not returning 0 here so that objects with the same size (but
        // different paths) are still added to the TreeSet.
      }
      return super.compareTo(obj);
    }
  }
}
