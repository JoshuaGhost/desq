package de.uni_mannheim.desq.io;

import de.uni_mannheim.desq.mining.Pattern;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Collection;

/**
 * Created by rgemulla on 16.08.2016.
 */
public class CountPatternWriter extends PatternWriter {
    long count = 0;
    long totalFrequency = 0;

    @Override
    public void write(IntList itemFids, long frequency) {
        count++;
        totalFrequency += frequency;
    }

    public void writeReverse(IntList reverseItemFids, long frequency) {
        count++;
        totalFrequency += frequency;
    }

    @Override
    public void write(int[] itemFids, long frequency) {
        count++;
        totalFrequency += frequency;
    }

    @Override
    public void write(Pattern pattern) {
        count++;
        totalFrequency += pattern.getFrequency();
    }

    public long getCount() {
        return count;
    }

    public long getTotalFrequency() {
        return totalFrequency;
    }
}
