/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import geotrellis.util.annotations.experimental

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.mapreduce.{AbstractInputFormat, AccumuloInputFormat, InputFormatBase, RangeInputSplit}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.util.{Pair => AccPair}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce._
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.SimpleFeatureDeserializers
import org.locationtech.geomesa.jobs.{GeoMesaConfigurator, JobUtils}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import java.io._
import java.lang.Float._
import java.net.{URL, URLClassLoader}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable

/**
  * @define experimental <span class="badge badge-red" style="float: right;">EXPERIMENTAL</span>@experimental
  */
@experimental object GeoMesaInputFormat extends LazyLogging {

  val SYS_PROP_SPARK_LOAD_CP = "org.locationtech.geomesa.spark.load-classpath"

  @experimental def configure(job: Job,
                dsParams: Map[String, String],
                featureTypeName: String,
                filter: Option[String] = None,
                transform: Option[Array[String]] = None): Unit = {
    val ecql = filter.map(ECQL.toFilter).getOrElse(Filter.INCLUDE)
    val trans = transform.getOrElse(Query.ALL_NAMES)
    val query = new Query(featureTypeName, ecql, trans)
    configure(job, dsParams, query)
  }

  /**
    * $experimental Configure the input format.
    *
    * This is a single method, as we have to calculate several things to pass to the underlying
    * AccumuloInputFormat, and there is not a good hook to indicate when the config is finished.
    */
  @experimental def configure(job: Job, dsParams: Map[String, String], query: Query): Unit = {

    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]
    assert(ds != null, "Invalid data store parameters")

    // set up the underlying accumulo input format
    val user = AccumuloDataStoreParams.userParam.lookUp(dsParams).asInstanceOf[String]
    val password = AccumuloDataStoreParams.passwordParam.lookUp(dsParams).asInstanceOf[String]
    InputFormatBaseAdapter.setConnectorInfo(job, user, new PasswordToken(password.getBytes))

    val instance = AccumuloDataStoreParams.instanceIdParam.lookUp(dsParams).asInstanceOf[String]
    val zookeepers = AccumuloDataStoreParams.zookeepersParam.lookUp(dsParams).asInstanceOf[String]
    if(AccumuloDataStoreParams.mockParam.lookUp(dsParams).asInstanceOf[String] == "true")
      AbstractInputFormat.setMockInstance(job, instance)
    else
      InputFormatBaseAdapter.setZooKeeperInstance(job, instance, zookeepers)

    val auths = Option(AccumuloDataStoreParams.authsParam.lookUp(dsParams).asInstanceOf[String])
    auths.foreach(a => InputFormatBaseAdapter.setScanAuthorizations(job, new Authorizations(a.split(","): _*)))

    val featureTypeName = query.getTypeName

    // get the query plan to set up the iterators, ranges, etc
    val queryPlan = JobUtils.getSingleQueryPlan(ds, query)

    // use the query plan to set the accumulo input format options
    InputFormatBase.setInputTableName(job, queryPlan.table)
    if (queryPlan.ranges.nonEmpty) {
      InputFormatBase.setRanges(job, queryPlan.ranges)
    }
    if (queryPlan.columnFamilies.nonEmpty) {
      InputFormatBase.fetchColumns(job, queryPlan.columnFamilies.map(cf => new AccPair[Text, Text](cf, null)))
    }
    queryPlan.iterators.foreach(InputFormatBase.addIterator(job, _))

    // auto adjust ranges - this ensures that each split created will have a single location, which we want
    // for the GeoMesaInputFormat below
    InputFormatBase.setAutoAdjustRanges(job, true)

    // also set the datastore parameters so we can access them later
    val conf = job.getConfiguration

    GeoMesaConfigurator.setSerialization(conf)
    GeoMesaConfigurator.setTable(conf, queryPlan.table)
    GeoMesaConfigurator.setDataStoreInParams(conf, dsParams)
    GeoMesaConfigurator.setFeatureType(conf, featureTypeName)
    if (query.getFilter != Filter.INCLUDE) {
      GeoMesaConfigurator.setFilter(conf, ECQL.toCQL(query.getFilter))
    }
    query.getHints.getTransformSchema.foreach(GeoMesaConfigurator.setTransformSchema(conf, _))

    ds.dispose()
  }

  /**
    * $experimental This takes any jars that have been loaded by spark
    * in the context classloader and makes them available to the
    * general classloader. This is required as not all classes (even
    * spark ones) check the context classloader.
    */
  @experimental def ensureSparkClasspath(): Unit = {
    val sysLoader = ClassLoader.getSystemClassLoader
    val ccl = Thread.currentThread().getContextClassLoader
    if (ccl == null || !ccl.getClass.getCanonicalName.startsWith("org.apache.spark.")) {
      logger.debug("No spark context classloader found")
    } else if (!ccl.isInstanceOf[URLClassLoader]) {
      logger.warn(s"Found context classloader, but can't handle type ${ccl.getClass.getCanonicalName}")
    } else if (!sysLoader.isInstanceOf[URLClassLoader]) {
      logger.warn(s"Found context classloader, but can't add to type ${sysLoader.getClass.getCanonicalName}")
    } else {
      // hack to get around protected visibility of addURL
      // this might fail if there is a security manager present
      val addUrl = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
      addUrl.setAccessible(true)
      val sysUrls = sysLoader.asInstanceOf[URLClassLoader].getURLs.map(_.toString).toSet
      val (dupeUrls, newUrls) = ccl.asInstanceOf[URLClassLoader].getURLs.filterNot(_.toString.contains("__app__.jar")).partition(url => sysUrls.contains(url.toString))
      newUrls.foreach(addUrl.invoke(sysLoader, _))
      logger.debug(s"Loaded ${newUrls.length} urls from context classloader into system classloader " +
        s"and ignored ${dupeUrls.length} that are already loaded")
    }
  }
}

/**
  * Input format that allows processing of simple features from GeoMesa based on a CQL query
  *
  * @define experimental <span class="badge badge-red" style="float: right;">EXPERIMENTAL</span>@experimental
  */
@experimental class GeoMesaInputFormat extends InputFormat[Text, SimpleFeature] with LazyLogging {

  val delegate = new AccumuloInputFormat

  var sft: SimpleFeatureType = null
  var encoding: SerializationType = null
  var desiredSplitCount: Int = -1
  var table: GeoMesaTable = null

  @experimental private def init(conf: Configuration) = if (sft == null) {
    val params = GeoMesaConfigurator.getDataStoreInParams(conf)
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
    sft = ds.getSchema(GeoMesaConfigurator.getFeatureType(conf))
    encoding = ds.getFeatureEncoding(sft)
    desiredSplitCount = GeoMesaConfigurator.getDesiredSplits(conf)
    val tableName = GeoMesaConfigurator.getTable(conf)
    table = GeoMesaTable.getTables(sft).find(t => tableName.endsWith(t.suffix)).getOrElse {
      throw new RuntimeException(s"Couldn't find input table $tableName")
    }
    ds.dispose()
  }

  /**
    * Gets splits for a job.
    *
    * Our delegated AccumuloInputFormat creates a split for each range - because we set a lot of ranges in
    * geomesa, that creates too many mappers. Instead, we try to group the ranges by tservers. We use the
    * number of shards in the schema as a proxy for number of tservers.
    */
  @experimental override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    init(context.getConfiguration)
    val accumuloSplits = delegate.getSplits(context)
    // fallback on creating 2 mappers per node if desiredSplits is unset.
    // Account for case where there are less splits than shards
    val groupSize = if (desiredSplitCount > 0) {
      Some(Math.max(1, accumuloSplits.length / desiredSplitCount))
    } else {
      None
    }
    val splitsSet = accumuloSplits.groupBy(_.getLocations()(0)).flatMap { case (location, splits) =>
      val size = groupSize.getOrElse(Math.max(1, splits.length / 2))
      splits.grouped(size).map { group =>
        val split = new GroupedSplit()
        split.location = location
        split.splits.append(group.map(_.asInstanceOf[RangeInputSplit]): _*)
        split
      }
    }

    logger.debug(s"Got ${splitsSet.toList.length} splits" +
      s" using desired=$desiredSplitCount from ${accumuloSplits.length}")
    splitsSet.toList
  }

  @experimental override def createRecordReader(split: InputSplit, context: TaskAttemptContext) = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    init(context.getConfiguration)
    val splits = split.asInstanceOf[GroupedSplit].splits
    val readers = splits.map(delegate.createRecordReader(_, context)).toArray
    val schema = GeoMesaConfigurator.getTransformSchema(context.getConfiguration).getOrElse(sft)
    val (serializationOptions, hasId) = if (sft.getSchemaVersion < 9) {
      (SerializationOptions.none, true)
    } else {
      (SerializationOptions.withoutId, false)
    }
    val decoder = SimpleFeatureDeserializers(schema, encoding, serializationOptions)
    new GeoMesaRecordReader(sft, table, readers, hasId, decoder)
  }
}

/**
  * Record reader that delegates to accumulo record readers and
  * transforms the key/values coming back into simple features.
  *
  * @param readers
  *
  * @define experimental <span class="badge badge-red" style="float: right;">EXPERIMENTAL</span>@experimental
  */
@experimental class GeoMesaRecordReader(sft: SimpleFeatureType,
                          table: GeoMesaTable,
                          readers: Array[RecordReader[Key, Value]],
                          hasId: Boolean,
                          decoder: org.locationtech.geomesa.features.SimpleFeatureSerializer)
  extends RecordReader[Text, SimpleFeature] {

  var currentFeature: SimpleFeature = null
  var readerIndex: Int = -1
  var currentReader: Option[RecordReader[Key, Value]] = None

  val getId = table.getIdFromRow(sft)

  @experimental override def initialize(split: InputSplit, context: TaskAttemptContext) = {
    val splits = split.asInstanceOf[GroupedSplit].splits
    var i = 0
    while (i < splits.length) {
      readers(i).initialize(splits(i), context)
      i = i + 1
    }

    // queue up our first reader
    nextReader()
  }

  /**
    * Advances to the next delegate reader
    */
  @experimental private[this] def nextReader() = {
    readerIndex = readerIndex + 1
    if (readerIndex < readers.length) {
      currentReader = Some(readers(readerIndex))
    } else {
      currentReader = None
    }
  }

  @experimental override def getProgress = if (readers.length == 0) 1f else if (readerIndex < 0) 0f else {
    val readersProgress = readerIndex * 1f / readers.length
    val readerProgress = currentReader.map(_.getProgress / readers.length).filterNot(isNaN).getOrElse(0f)
    readersProgress + readerProgress
  }

  @experimental override def nextKeyValue() = nextKeyValueInternal()

  /**
    * Get the next key value from the underlying reader, incrementing the reader when required
    */
  @tailrec @experimental private def nextKeyValueInternal(): Boolean =
  currentReader match {
    case None => false
    case Some(reader) =>
      if (reader.nextKeyValue()) {
        currentFeature = decoder.deserialize(reader.getCurrentValue.get())
        if (!hasId) {
          currentFeature.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(reader.getCurrentKey.getRow))
        }
        true
      } else {
        nextReader()
        nextKeyValueInternal()
      }
  }

  @experimental override def getCurrentValue = currentFeature

  @experimental override def getCurrentKey = new Text(currentFeature.getID)

  @experimental override def close() = {} // delegate Accumulo readers have a no-op close
}

/**
  * Input split that groups a series of RangeInputSplits. Has to
  * implement Hadoop Writable, thus the vars and mutable state.
  *
  * @define experimental <span class="badge badge-red" style="float: right;">EXPERIMENTAL</span>@experimental
  */
@experimental class GroupedSplit extends InputSplit with Writable {

  // if we're running in spark, we need to load the context classpath before anything else,
  // otherwise we get classloading and serialization issues
  sys.env.get(GeoMesaInputFormat.SYS_PROP_SPARK_LOAD_CP).filter(_.toBoolean).foreach { _ =>
    GeoMesaInputFormat.ensureSparkClasspath()
  }

  var location: String = null
  var splits: ArrayBuffer[RangeInputSplit] = ArrayBuffer.empty

  @experimental override def getLength =  splits.foldLeft(0L)((l: Long, r: RangeInputSplit) => l + r.getLength)

  @experimental override def getLocations = if (location == null) Array.empty else Array(location)

  @experimental override def write(out: DataOutput) = {
    out.writeUTF(location)
    out.writeInt(splits.length)
    splits.foreach(_.write(out))
  }

  @experimental override def readFields(in: DataInput) = {
    location = in.readUTF()
    splits.clear()
    var i = 0
    val size = in.readInt()
    while (i < size) {
      val split = new RangeInputSplit()
      split.readFields(in)
      splits.append(split)
      i = i + 1
    }
  }

  @experimental override def toString = s"mapreduce.GroupedSplit[$location](${splits.length})"
}
