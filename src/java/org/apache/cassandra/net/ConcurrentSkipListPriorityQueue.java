/*
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

package org.apache.cassandra.net;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by reda on 11/01/16.
 */
public class ConcurrentSkipListPriorityQueue<T> implements Queue<T>
{

    private ConcurrentSkipListMap<T, Boolean> values;

    public ConcurrentSkipListPriorityQueue(Comparator<? super T> comparator) {
        values = new ConcurrentSkipListMap<T, Boolean>(comparator);
    }

    public ConcurrentSkipListPriorityQueue() {
        values = new ConcurrentSkipListMap<T, Boolean>();
    }

    @Override
    public boolean add(T e) {
        values.put(e, Boolean.TRUE);
        return true;
    }

    @Override
    public boolean offer(T e) {
        return add(e);
    }

    @Override
    public T remove() {
        final T v = values.firstKey();
        values.remove(v);
        return v;
    }

    @Override
    public T poll() {
        if (values.isEmpty()) {
            return null;
        }

        return remove();
    }

    @Override
    public T element() {
        return values.firstKey();
    }

    @Override
    public T peek() {
        if (values.isEmpty()) {
            return null;
        }

        return element();
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return values.containsKey(o);
    }

    @Override
    public Iterator<T> iterator() {
        return values.keySet().iterator();
    }

    @Override
    public Object[] toArray() {
        return values.keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return values.keySet().toArray(a);
    }

    @Override
    public boolean remove(Object o) {
        return values.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return values.keySet().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {

        boolean changed = false;

        for (T i : c) {
            changed |= add(i);
        }

        return changed;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return values.keySet().removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return values.keySet().retainAll(c);
    }

    @Override
    public void clear() {
        values.clear();
    }

}