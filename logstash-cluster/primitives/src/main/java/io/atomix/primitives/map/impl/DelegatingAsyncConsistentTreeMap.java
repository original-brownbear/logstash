/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.atomix.primitives.map.impl;

import io.atomix.primitives.TransactionId;
import io.atomix.primitives.TransactionLog;
import io.atomix.primitives.impl.DelegatingDistributedPrimitive;
import io.atomix.primitives.map.AsyncConsistentTreeMap;
import io.atomix.primitives.map.MapEventListener;
import io.atomix.time.Version;
import io.atomix.time.Versioned;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link AsyncConsistentTreeMap} that delegates control to another instance
 * of {@link AsyncConsistentTreeMap}.
 */
public class DelegatingAsyncConsistentTreeMap<K, V>
    extends DelegatingDistributedPrimitive
    implements AsyncConsistentTreeMap<K, V> {

  private final AsyncConsistentTreeMap<K, V> delegateMap;

  DelegatingAsyncConsistentTreeMap(AsyncConsistentTreeMap<K, V> delegateMap) {
    super(delegateMap);
    this.delegateMap = checkNotNull(delegateMap,
        "delegate map cannot be null");
  }

  @Override
  public CompletableFuture<K> firstKey() {
    return delegateMap.firstKey();
  }

  @Override
  public CompletableFuture<K> lastKey() {
    return delegateMap.lastKey();
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> ceilingEntry(K key) {
    return delegateMap.ceilingEntry(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> floorEntry(K key) {
    return delegateMap.floorEntry(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> higherEntry(K key) {
    return delegateMap.higherEntry(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> lowerEntry(K key) {
    return delegateMap.lowerEntry(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> firstEntry() {
    return delegateMap.firstEntry();
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> lastEntry() {
    return delegateMap.lastEntry();
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> pollFirstEntry() {
    return delegateMap.pollFirstEntry();
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> pollLastEntry() {
    return delegateMap.pollLastEntry();
  }

  @Override
  public CompletableFuture<K> lowerKey(K key) {
    return delegateMap.lowerKey(key);
  }

  @Override
  public CompletableFuture<K> floorKey(K key) {
    return delegateMap.floorKey(key);
  }

  @Override
  public CompletableFuture<K> ceilingKey(K key) {
    return delegateMap.ceilingKey(key);
  }

  @Override
  public CompletableFuture<K> higherKey(K key) {
    return delegateMap.higherKey(key);
  }

  @Override
  public CompletableFuture<NavigableSet<K>> navigableKeySet() {
    return delegateMap.navigableKeySet();
  }

  @Override
  public CompletableFuture<NavigableMap<K, V>> subMap(
      K upperKey,
      K lowerKey,
      boolean inclusiveUpper,
      boolean inclusiveLower) {
    return delegateMap.subMap(upperKey, lowerKey,
        inclusiveUpper, inclusiveLower);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return delegateMap.size();
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return delegateMap.containsKey(key);
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V value) {
    return delegateMap.containsValue(value);
  }

  @Override
  public CompletableFuture<Versioned<V>> get(K key) {
    return delegateMap.get(key);
  }

  @Override
  public CompletableFuture<Map<K, Versioned<V>>> getAllPresent(Iterable<K> keys) {
    return delegateMap.getAllPresent(keys);
  }

  @Override
  public CompletableFuture<Versioned<V>> getOrDefault(K key, V defaultValue) {
    return delegateMap.getOrDefault(key, defaultValue);
  }

  @Override
  public CompletableFuture<Versioned<V>> computeIf(
      K key,
      Predicate<? super V> condition,
      BiFunction<? super K, ? super V,
          ? extends V> remappingFunction) {
    return delegateMap.computeIf(key, condition, remappingFunction);
  }

  @Override
  public CompletableFuture<Versioned<V>> put(K key, V value) {
    return delegateMap.put(key, value);
  }

  @Override
  public CompletableFuture<Versioned<V>> putAndGet(K key, V value) {
    return delegateMap.putAndGet(key, value);
  }

  @Override
  public CompletableFuture<Versioned<V>> remove(K key) {
    return delegateMap.remove(key);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return delegateMap.clear();
  }

  @Override
  public CompletableFuture<Set<K>> keySet() {
    return delegateMap.keySet();
  }

  @Override
  public CompletableFuture<Collection<Versioned<V>>> values() {
    return delegateMap.values();
  }

  @Override
  public CompletableFuture<Set<Map.Entry<K, Versioned<V>>>> entrySet() {
    return delegateMap.entrySet();
  }

  @Override
  public CompletableFuture<Versioned<V>> putIfAbsent(K key, V value) {
    return delegateMap.putIfAbsent(key, value);
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return delegateMap.remove(key, value);
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, long version) {
    return delegateMap.remove(key, version);
  }

  @Override
  public CompletableFuture<Versioned<V>> replace(K key, V value) {
    return delegateMap.replace(key, value);
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue,
                                            V newValue) {
    return delegateMap.replace(key, oldValue, newValue);
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, long oldVersion,
                                            V newValue) {
    return delegateMap.replace(key, oldVersion, newValue);
  }

  @Override
  public CompletableFuture<Void> addListener(
      MapEventListener<K, V> listener, Executor executor) {
    return delegateMap.addListener(listener, executor);
  }

  @Override
  public CompletableFuture<Void> removeListener(
      MapEventListener<K, V> listener) {
    return delegateMap.removeListener(listener);
  }

  @Override
  public CompletableFuture<Version> begin(TransactionId transactionId) {
    return delegateMap.begin(transactionId);
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<K, V>> transactionLog) {
    return delegateMap.prepare(transactionLog);
  }

  @Override
  public CompletableFuture<Boolean> prepareAndCommit(TransactionLog<MapUpdate<K, V>> transactionLog) {
    return delegateMap.prepareAndCommit(transactionLog);
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return delegateMap.commit(transactionId);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return delegateMap.rollback(transactionId);
  }
}
