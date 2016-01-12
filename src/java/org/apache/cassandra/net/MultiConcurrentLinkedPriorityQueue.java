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

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by reda on 12/01/16.
 */
public class MultiConcurrentLinkedPriorityQueue<E> extends AbstractQueue<E>
{
    Random r;
    final int qcount = 3;
    List<ConcurrentLinkedQueue<E>> queues;

    public MultiConcurrentLinkedPriorityQueue()
    {
        r =  new Random();
        queues = new ArrayList<ConcurrentLinkedQueue<E>>();
        for (int i=0; i<qcount; i++)
        {
            queues.add(new ConcurrentLinkedQueue<E>());
        }
    }

    /**
     * Returns an iterator over the elements contained in this collection.
     *
     * @return an iterator over the elements contained in this collection
     */
    public Iterator<E> iterator()
    {
        throw new UnsupportedOperationException();
    }

    public int size()
    {
        int size = 0;
        for (int i=0; i<qcount; i++)
            size += queues.get(i).size();
        return size;
    }

    @Override
    public E poll() {
        int choice = r.nextInt(qcount);
        for(int i=0; i<qcount; i++)
        {
            if(queues.get(choice).peek() != null)
                return queues.get(choice).poll();
            choice = (choice+1)%qcount;
        }
        return null;
    }

    @Override
    public boolean isEmpty()
    {
        for(int i=0; i<qcount; i++)
        {
            if(queues.get(i).isEmpty())
                return true;
        }
        return false;
    }

    /**
     * Retrieves, but does not remove, the head of this queue,
     * or returns <tt>null</tt> if this queue is empty.
     *
     * @return the head of this queue, or <tt>null</tt> if this queue is empty
     */
    public E peek()
    {
        for(int i=0; i<qcount; i++)
        {
            E peekabo = queues.get(i).peek();
            if(peekabo != null)
                return peekabo;
        }
        return null;
    }

    @Override
    public boolean offer(E key) {
        if (key == null)
            throw new NullPointerException();
        int choice = r.nextInt(qcount);
        queues.get(choice).offer(key);
        return true;
    }

    public boolean add(E e) {
        return offer(e);
    }
}
