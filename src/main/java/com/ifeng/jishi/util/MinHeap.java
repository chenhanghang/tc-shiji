package com.ifeng.jishi.util;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * 优先级队列
 */
public class MinHeap implements Serializable {
    private int max_num;
    private PriorityQueue<Tuple2<String, Double>> heap = new PriorityQueue<>(new SerializableComparator());

    class SerializableComparator implements Comparator<Tuple2<String, Double>>,Serializable{
        @Override
        public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
            return o1._2().compareTo(o2._2());
        }
    }

    public MinHeap(int max_num) {
        this.max_num = max_num;
    }

    public MinHeap add(Tuple2<String, Double> t) {
        for(Tuple2<String, Double> temp : heap){
            if(temp._1.equals(t._1)) return this;
        }
        if (heap.size() < max_num) {
            heap.add(t);
        } else {
            Double min = heap.peek()._2;
            if (t._2 > min) {
                heap.poll();
                heap.add(t);
            }
        }
        return this;
    }

    public PriorityQueue<Tuple2<String, Double>> getHeap(){
        return heap;
    }

    public MinHeap addAll(MinHeap new_heap){
        for(Tuple2<String, Double>t : new_heap.getHeap()){
            add(t);
        }
        return this;
    }

    public List<Tuple2<String, Double>> getSortedItems() {
        return heap
                .stream()
                .sorted((a, b) -> b._2.compareTo(a._2))
                .collect(Collectors.toList());
    }

}
