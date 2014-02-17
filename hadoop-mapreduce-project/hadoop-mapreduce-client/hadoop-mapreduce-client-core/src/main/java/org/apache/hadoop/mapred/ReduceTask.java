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
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import java.nio.MappedByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter;
import org.apache.hadoop.mapreduce.task.reduce.Shuffle;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.io.WritableUtils;

/** A Reduce task. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ReduceTask extends Task {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (ReduceTask.class,
       new WritableFactory() {
         public Writable newInstance() { return new ReduceTask(); }
       });
  }
  
  private static final Log LOG = LogFactory.getLog(ReduceTask.class.getName());
  private int numMaps;

  private CompressionCodec codec;


  { 
    getProgress().setStatus("reduce"); 
    setPhase(TaskStatus.Phase.SHUFFLE);        // phase to start with 
  }

  private Progress copyPhase;
  private Progress sortPhase;
  private Progress reducePhase;
  private Counters.Counter shuffledMapsCounter = 
    getCounters().findCounter(TaskCounter.SHUFFLED_MAPS);
  private Counters.Counter reduceShuffleBytes = 
    getCounters().findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES);
  private Counters.Counter reduceInputKeyCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_INPUT_GROUPS);
  private Counters.Counter reduceInputValueCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_INPUT_RECORDS);
  private Counters.Counter reduceOutputCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);
  private Counters.Counter reduceCombineInputCounter =
    getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
  private Counters.Counter reduceCombineOutputCounter =
    getCounters().findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);
  private Counters.Counter fileOutputByteCounter =
    getCounters().findCounter(FileOutputFormatCounter.BYTES_WRITTEN);

  // A custom comparator for map output files. Here the ordering is determined
  // by the file's size and path. In case of files with same size and different
  // file paths, the first parameter is considered smaller than the second one.
  // In case of files with same size and path are considered equal.
    
    
    public iterativeComputing iterate;

    
  public ReduceTask() {
    super();
  }

  public ReduceTask(String jobFile, TaskAttemptID taskId,
                    int partition, int numMaps, int numSlotsRequired) {
    super(jobFile, taskId, partition, numSlotsRequired);
    this.numMaps = numMaps;
  }
  
  private CompressionCodec initCodec() {
    // check if map-outputs are to be compressed
    if (conf.getCompressMapOutput()) {
      Class<? extends CompressionCodec> codecClass =
        conf.getMapOutputCompressorClass(DefaultCodec.class);
      return ReflectionUtils.newInstance(codecClass, conf);
    } 

    return null;
  }

  @Override
  public boolean isMapTask() {
    return false;
  }

  public int getNumMaps() { return numMaps; }
  
  /**
   * Localize the given JobConf to be specific for this task.
   */
  @Override
  public void localizeConfiguration(JobConf conf) throws IOException {
    super.localizeConfiguration(conf);
    conf.setNumMapTasks(numMaps);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(numMaps);                        // write the number of maps
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    numMaps = in.readInt();
  }
  
  private class ReduceValuesIterator<KEY,VALUE> 
          extends ValuesIterator<KEY,VALUE> {
    public ReduceValuesIterator (RawKeyValueIterator in,
                                 RawComparator<KEY> comparator, 
                                 Class<KEY> keyClass,
                                 Class<VALUE> valClass,
                                 Configuration conf, Progressable reporter)
      throws IOException {
      super(in, comparator, keyClass, valClass, conf, reporter);
    }

    @Override
    public VALUE next() {
      reduceInputValueCounter.increment(1);
      return moveToNext();
    }
    
    protected VALUE moveToNext() {
      return super.next();
    }
    
    public void informReduceProgress() {
      reducePhase.set(super.in.getProgress().getProgress()); // update progress
      reporter.progress();
    }
  }

  private class SkippingReduceValuesIterator<KEY,VALUE> 
     extends ReduceValuesIterator<KEY,VALUE> {
     private SkipRangeIterator skipIt;
     private TaskUmbilicalProtocol umbilical;
     private Counters.Counter skipGroupCounter;
     private Counters.Counter skipRecCounter;
     private long grpIndex = -1;
     private Class<KEY> keyClass;
     private Class<VALUE> valClass;
     private SequenceFile.Writer skipWriter;
     private boolean toWriteSkipRecs;
     private boolean hasNext;
     private TaskReporter reporter;
     
     public SkippingReduceValuesIterator(ShmKVIterator in,
         RawComparator<KEY> comparator, Class<KEY> keyClass,
         Class<VALUE> valClass, Configuration conf, TaskReporter reporter,
         TaskUmbilicalProtocol umbilical) throws IOException {
       super(in, comparator, keyClass, valClass, conf, reporter);
       this.umbilical = umbilical;
       this.skipGroupCounter = 
         reporter.getCounter(TaskCounter.REDUCE_SKIPPED_GROUPS);
       this.skipRecCounter = 
         reporter.getCounter(TaskCounter.REDUCE_SKIPPED_RECORDS);
       this.toWriteSkipRecs = toWriteSkipRecs() &&  
         SkipBadRecords.getSkipOutputPath(conf)!=null;
       this.keyClass = keyClass;
       this.valClass = valClass;
       this.reporter = reporter;
       skipIt = getSkipRanges().skipRangeIterator();
       mayBeSkip();
     }
     
     public void nextKey() throws IOException {
       super.nextKey();
       mayBeSkip();
     }
     
     public boolean more() { 
       return super.more() && hasNext; 
     }
     
     private void mayBeSkip() throws IOException {
       hasNext = skipIt.hasNext();
       if(!hasNext) {
         LOG.warn("Further groups got skipped.");
         return;
       }
       grpIndex++;
       long nextGrpIndex = skipIt.next();
       long skip = 0;
       long skipRec = 0;
       while(grpIndex<nextGrpIndex && super.more()) {
         while (hasNext()) {
           VALUE value = moveToNext();
           if(toWriteSkipRecs) {
             writeSkippedRec(getKey(), value);
           }
           skipRec++;
         }
         super.nextKey();
         grpIndex++;
         skip++;
       }
       
       //close the skip writer once all the ranges are skipped
       if(skip>0 && skipIt.skippedAllRanges() && skipWriter!=null) {
         skipWriter.close();
       }
       skipGroupCounter.increment(skip);
       skipRecCounter.increment(skipRec);
       reportNextRecordRange(umbilical, grpIndex);
     }
     
     @SuppressWarnings("unchecked")
     private void writeSkippedRec(KEY key, VALUE value) throws IOException{
       if(skipWriter==null) {
         Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
         Path skipFile = new Path(skipDir, getTaskID().toString());
         skipWriter = SequenceFile.createWriter(
               skipFile.getFileSystem(conf), conf, skipFile,
               keyClass, valClass, 
               CompressionType.BLOCK, reporter);
       }
       skipWriter.append(key, value);
     }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException, InterruptedException, ClassNotFoundException {
    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());

    if (isMapOrReduce()) {
      copyPhase = getProgress().addPhase("copy");
      sortPhase  = getProgress().addPhase("sort");
      reducePhase = getProgress().addPhase("reduce");
    }
    // start thread that will handle communication with parent
    TaskReporter reporter = startReporter(umbilical);
    
    boolean useNewApi = job.getUseNewReducer();
    initialize(job, getJobID(), reporter, useNewApi);
    
    // check if it is a cleanupJobTask
    if (jobCleanup) {
      runJobCleanupTask(umbilical, reporter);
      return;
    }
    if (jobSetup) {
      runJobSetupTask(umbilical, reporter);
      return;
    }
    if (taskCleanup) {
      runTaskCleanupTask(umbilical, reporter);
      return;
    }
    
    // Initialize the codec
    codec = initCodec();

    ShuffleConsumerPlugin shuffleConsumerPlugin = null; 
    
    Class keyClass = job.getMapOutputKeyClass();
    Class valueClass = job.getMapOutputValueClass();
    RawComparator comparator = job.getOutputValueGroupingComparator();
    
    iterate = new iterativeComputing(job, umbilical, reporter, keyClass, valueClass);
    
    boolean isLocal = false; 
    // local if
    // 1) framework == local or
    // 2) framework == null and job tracker address == local
    String framework = job.get(MRConfig.FRAMEWORK_NAME);
    String masterAddr = job.get(MRConfig.MASTER_ADDRESS, "local");
    if ((framework == null && masterAddr.equals("local"))
        || (framework != null && framework.equals(MRConfig.LOCAL_FRAMEWORK_NAME))) {
      isLocal = true;
    }
    
    if (!isLocal) {
      Class combinerClass = conf.getCombinerClass();
      CombineOutputCollector combineCollector = 
        (null != combinerClass) ? 
 	     new CombineOutputCollector(reduceCombineOutputCounter, reporter, conf) : null;

      Class<? extends ShuffleConsumerPlugin> clazz =
            job.getClass(MRConfig.SHUFFLE_CONSUMER_PLUGIN, Shuffle.class, ShuffleConsumerPlugin.class);
						
      shuffleConsumerPlugin = ReflectionUtils.newInstance(clazz, job);
      LOG.info("Using ShuffleConsumerPlugin: " + shuffleConsumerPlugin);

      ShuffleConsumerPlugin.Context shuffleContext = 
        new ShuffleConsumerPlugin.Context(getTaskID(), job, FileSystem.getLocal(job), umbilical, 
                    super.lDirAlloc, reporter, codec, 
                    combinerClass, combineCollector, 
                    spilledRecordsCounter, reduceCombineInputCounter,
                    shuffledMapsCounter,
                    reduceShuffleBytes, failedShuffleCounter,
                    mergedMapOutputsCounter,
                    taskStatus, copyPhase, sortPhase, this,
                    mapOutputFile);
      shuffleConsumerPlugin.init(shuffleContext);
      shuffleConsumerPlugin.run();
    }
    
    sortPhase.complete();                         // sort is complete
    setPhase(TaskStatus.Phase.REDUCE); 
    statusUpdate(umbilical);

    /* write mr output */
    iterate.writeOutput();
    iterate.cleanup();

    if (shuffleConsumerPlugin != null) {
      shuffleConsumerPlugin.close();
    }

    done(umbilical, reporter);
  }

  static class OldTrackingRecordWriter<K, V> implements RecordWriter<K, V> {

    private final RecordWriter<K, V> real;
    private final org.apache.hadoop.mapred.Counters.Counter reduceOutputCounter;
    private final org.apache.hadoop.mapred.Counters.Counter fileOutputByteCounter;
    private final List<Statistics> fsStats;

    @SuppressWarnings({ "deprecation", "unchecked" })
    public OldTrackingRecordWriter(ReduceTask reduce, JobConf job,
        TaskReporter reporter, String finalName) throws IOException {
      this.reduceOutputCounter = reduce.reduceOutputCounter;
      this.fileOutputByteCounter = reduce.fileOutputByteCounter;
      List<Statistics> matchedStats = null;
      if (job.getOutputFormat() instanceof FileOutputFormat) {
        matchedStats = getFsStatistics(FileOutputFormat.getOutputPath(job), job);
      }
      fsStats = matchedStats;

      FileSystem fs = FileSystem.get(job);
      long bytesOutPrev = getOutputBytes(fsStats);
      this.real = job.getOutputFormat().getRecordWriter(fs, job, finalName,
          reporter);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    @Override
    public void write(K key, V value) throws IOException {
      long bytesOutPrev = getOutputBytes(fsStats);
      real.write(key, value);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      reduceOutputCounter.increment(1);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      long bytesOutPrev = getOutputBytes(fsStats);
      real.close(reporter);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    private long getOutputBytes(List<Statistics> stats) {
      if (stats == null) return 0;
      long bytesWritten = 0;
      for (Statistics stat: stats) {
        bytesWritten = bytesWritten + stat.getBytesWritten();
      }
      return bytesWritten;
    }
  }

    public class iterativeComputing  {
	
	public ShmKVIterator rawIter;
	public org.apache.hadoop.mapreduce.Reducer.Context 
	    reducerContext;
	public Progress progress;
	// make a reducer
	//	public org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer;
	public ArrayList<org.apache.hadoop.mapreduce.Reducer> reducerList;
	//	public org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> trackedRW;
	org.apache.hadoop.mapreduce.TaskAttemptContext taskContext;
	JobConf job;
	public org.apache.hadoop.mapreduce.RecordWriter trackedRW;
	private SharedHashMap shmFinal; 
	MappedByteBuffer mbf;
	private int arrListIndex = 0;
	//	private DataInputBuffer indexInpBuffer = new DataInputBuffer();
	//	private byte[] result = new byte[4];
	private boolean userWriteOutput = false;
	Class<?> ReducerClazz;
	private int count = 0;

	public <INKEY,INVALUE,OUTKEY,OUTVALUE>
	    iterativeComputing(JobConf job, 
			       final TaskUmbilicalProtocol umbilical,
			       final TaskReporter reporter,
			       Class<INKEY> keyClass, 
			       Class<INVALUE> valueClass) throws IOException,InterruptedException, ClassNotFoundException {
	    // wrap value iterator to report progress.
	    this.job = job;
	    progress = new Progress();
	    progress.set((float)0.4);
	    ShmKVIterator rIter = new ShmKVIterator() {
		    public void close() throws IOException {
			rawIter.close();
		    }
		    public DataInputBuffer getKey() throws IOException {
			return rawIter.getKey();
		    }
		    public Progress getProgress() {
			//			return rawIter.getProgress();
			return progress;
		    }
		    public DataInputBuffer getValue() throws IOException {
			return rawIter.getValue();
		    }
		    public boolean next() throws IOException {
			boolean ret = rawIter.next();
			//			reducePhase.set(rawIter.getProgress().get());
			//			  reducePhase.set(progress.get());
			reporter.setProgress(progress.get());
			return ret;
		    }
		    public boolean isNextKeySame() {
			boolean ret = rawIter.isNextKeySame();
			return ret;
		    }
		    public boolean start() {
			boolean ret = rawIter.start();
			return ret;
		    }
		};
	    
	    //	    indexInpBuffer.reset(result, 0, 4); 
	    shmFinal = new SharedHashMap("shmFinal_" + getTaskID(), true, 64 * 1024);
	    mbf = shmFinal.getMappedByteBuf();
	    // make a task context so we can get the classes
	    taskContext = 
		new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(job, getTaskID(), reporter);
	    ReducerClazz = taskContext.getReducerClass();
	    try {
		if (ReducerClazz == ReducerClazz.getMethod("writeReduceOp").getDeclaringClass())
		    userWriteOutput = true;
	    } catch(NoSuchMethodException e) {
		LOG.info("not found");
	    }
	    
	    reducerList = new ArrayList<org.apache.hadoop.mapreduce.Reducer>();

	    trackedRW = new NewTrackingRecordWriter<OUTKEY, OUTVALUE>(ReduceTask.this, taskContext);
	      
	    job.setBoolean("mapred.skip.on", isSkipping());
	    reducerContext = createReduceContext(null, job, getTaskID(),
						 rIter, reduceInputKeyCounter, 
						 reduceInputValueCounter, 
						 trackedRW, committer,
						 reporter, keyClass,
						 valueClass);
	}
      
	/*	private void modifyInputBuffer(int i) {
	    result[0] = (byte) (i);
	    result[1] = (byte) (i >> 8);
	    result[2] = (byte) (i >> 16);
	    result[3] = (byte) (i >> 24); 
	}
	*/
	
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
	
	public <INKEY,INVALUE,OUTKEY,OUTVALUE>
	    void runShm(org.apache.hadoop.mapreduce.Reducer.Context  context) throws IOException, InterruptedException, ClassNotFoundException {
	    org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer;
	    DataInputBuffer keyBuf;
	    int index;
	    int offset, keyOff, valOff;
	    //	    setup(context);
	    long end = 0;
	    while (context.nextKey()) {
		keyBuf = context.getKeyBuf();
		index = getIndex(shmFinal.get(keyBuf));
		if (index == -1) {
		    reducer = (org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>) ReflectionUtils.newInstance(taskContext.getReducerClass(), job);
		    if (count == 0) {
			setup(reducer);
			count++;
		    }
		    reducerList.add(reducer);
		    
		    offset = shmFinal.getOffset();
		    //		    modifyInputBuffer(arrListIndex++);
		    valOff = writeShm(mbf, offset, arrListIndex++);
		    keyOff = writeShm(mbf, offset + 9, keyBuf);
		    
		    shmFinal.put(valOff, 4, keyOff, keyBuf.getLength());
		    //  shmFinal.put(keyBuf, indexInpBuffer);
		} else
		    reducer = reducerList.get(index);
		long start = System.currentTimeMillis();
		reducer.reduceShm((INKEY)context.getCurrentKey(), context.getValues(), context);
		end += (System.currentTimeMillis() - start);
	    }

	    LOG.info("shmreduce = " + end);
	    //	    System.out.println("shmreduce = " + (end-start));
	    //	    cleanup(context);
	}

	public void startProcessing(SharedHashMap shm) throws IOException, InterruptedException, ClassNotFoundException{
	    rawIter = shm.getIterator();
	    reducerContext.newIterator();

	    runShm(reducerContext);
	
	    // delete the file containing shm
	    shm.destroyLookupHash();
	}
    
	private void destroyShm() {
	    // reclaim the space 
	    System.gc();
	}
	
	public <INKEY,INVALUE,OUTKEY,OUTVALUE>
	    void writeOutput() throws IOException, InterruptedException {
	    ShmKVIterator iter;
	    int index;
	    org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer;
	    long start = System.currentTimeMillis();
	    if (userWriteOutput == true) {
		iter = shmFinal.getFinalIterator();
		//reducerContext.newIterator();
		iter.start();
		do {
		    index = getIndex(iter.getValue());
		    reducer = reducerList.get(index);
		    reducer.writeReduceOp((INKEY)reducerContext.deserializedKey(iter.getKey()), reducerContext);
		} while (iter.next());
		
		//	    reducer.writeOutput(reducerContext);
	    }	
	    long end = System.currentTimeMillis();
	    LOG.info("write time = " + (end - start));
	    trackedRW.close(reducerContext);
	}

	public void setup(org.apache.hadoop.mapreduce.Reducer reducer) throws IOException, InterruptedException {
	    reducer.setup(reducerContext);
	}

	public <INKEY,INVALUE,OUTKEY,OUTVALUE>
	    void cleanup() throws IOException, InterruptedException{
	    org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer; 
	    reducer = reducerList.get(0);
	    reducer.cleanup(reducerContext);
	}
    }
    
    
  static class NewTrackingRecordWriter<K,V> 
      extends org.apache.hadoop.mapreduce.RecordWriter<K,V> {
    private final org.apache.hadoop.mapreduce.RecordWriter<K,V> real;
    private final org.apache.hadoop.mapreduce.Counter outputRecordCounter;
    private final org.apache.hadoop.mapreduce.Counter fileOutputByteCounter;
    private final List<Statistics> fsStats;

    @SuppressWarnings("unchecked")
    NewTrackingRecordWriter(ReduceTask reduce,
        org.apache.hadoop.mapreduce.TaskAttemptContext taskContext)
        throws InterruptedException, IOException {
      this.outputRecordCounter = reduce.reduceOutputCounter;
      this.fileOutputByteCounter = reduce.fileOutputByteCounter;

      List<Statistics> matchedStats = null;
      if (reduce.outputFormat instanceof org.apache.hadoop.mapreduce.lib.output.FileOutputFormat) {
        matchedStats = getFsStatistics(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
            .getOutputPath(taskContext), taskContext.getConfiguration());
      }

      fsStats = matchedStats;

      long bytesOutPrev = getOutputBytes(fsStats);
      this.real = (org.apache.hadoop.mapreduce.RecordWriter<K, V>) reduce.outputFormat
          .getRecordWriter(taskContext);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
    InterruptedException {
      long bytesOutPrev = getOutputBytes(fsStats);
      real.close(context);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      long bytesOutPrev = getOutputBytes(fsStats);
      real.write(key,value);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      outputRecordCounter.increment(1);
    }

    private long getOutputBytes(List<Statistics> stats) {
      if (stats == null) return 0;
      long bytesWritten = 0;
      for (Statistics stat: stats) {
        bytesWritten = bytesWritten + stat.getBytesWritten();
      }
      return bytesWritten;
    }
  }

  private <OUTKEY, OUTVALUE>
  void closeQuietly(RecordWriter<OUTKEY, OUTVALUE> c, Reporter r) {
    if (c != null) {
      try {
        c.close(r);
      } catch (Exception e) {
        LOG.info("Exception in closing " + c, e);
      }
    }
  }
}
