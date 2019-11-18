package org.apache.storm.cassandra.query.builder;

import java.io.Serializable;
import java.util.concurrent.Callable;

public interface SerializableCallable<R> extends Callable<R>, Serializable {
}