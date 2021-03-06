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

import org.apache.log4j.Priority;

import org.apache.cassandra.concurrent.AbstractTracingAwareExecutorService;

/**
 * Created by reda on 12/01/16.
 */
public class MultiConcurrentLinkedPriorityQueue<E> extends AbstractQueue<E>
{
    Random r;
    int qcount;
    List<ConcurrentLinkedQueue<E>> queues;
    List<Integer> weights;
    int weightSum;

    public MultiConcurrentLinkedPriorityQueue(List<Integer> weights)
    {
        qcount = weights.size();
        for (Integer x: weights)
            weightSum += x;
        this.weights = weights;
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
    public synchronized E poll() {
        //TODO: It might be better to iteratively poll directly instead of peeking (and if non-null return)
        //Which approach is more expensive computationally (compared to synchronization)?
        boolean searching = false;
        int choice = r.nextInt(weightSum);
        int acc = 0;
        //System.out.println(choice);
        for (int i = 0; i < qcount; i++)
        {
            acc += weights.get(i);
            if (acc >= (choice + 1))
            {
                if (queues.get(i).peek() != null)
                    return queues.get(i).poll();
                else
                {
                    for (int j = 0; j < qcount; j++)
                    {
                        if (queues.get(j).peek() != null)
                            return queues.get(j).poll();
                    }
                    return null;
                }
            }
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
        if(key instanceof PriorityProvider)
        {
            PriorityTuple priority = ((PriorityProvider)key).getPriority();
            int chosenWeight = priority.left.intValue();
            int choice = weights.indexOf(chosenWeight);
            if(choice != -1)
            {
                queues.get(choice).offer(key);
                return true;
            }
        }
        int choice = r.nextInt(qcount);
        queues.get(choice).offer(key);
        return true;
    }

    public boolean add(E e) {
        return offer(e);
    }
}
