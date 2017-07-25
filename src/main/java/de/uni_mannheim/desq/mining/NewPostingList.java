/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;

/**
 *
 * @author Kai-Arne
 */
public class NewPostingList extends AbstractPostingList{

    private ByteArrayList data;
    
    public NewPostingList(){
        this.data = new ByteArrayList();
        this.noPostings = 0;
    }
    
    @Override
    protected void addNonNegativeIntIntern(int value) {
        assert value >= 0;
        assert size() > 0;

        while (true) {
            final int b = value & 0x7F;
            if (value == b) {
                data.add((byte)b);
                return;
            } else {
                data.add((byte)(b | 0x80));
                value >>>= 7;
            }
        }
    }
    
    @Override
    public void clear() {
        data.clear();
        noPostings = 0;
    }

    @Override
    public int noBytes() { return data.size(); }

    @Override
    public void trim() { data.trim(); }

    @Override
    public Object getData() { return this.data; }
    
    @Override
    public AbstractIterator iterator() { return new Iterator(this); }
    
    public final class Iterator extends AbstractIterator {
        public Iterator() {
            this.data = null;
            this.offset = 0;
        }

        /** Creates an iterator backed by the given data */
        public Iterator(ByteArrayList data) {
            this.data = data;
            this.offset = 0;
        }

        /** Creates an iterator backed by the given posting list */
        public Iterator(AbstractPostingList postingList) {
            this.data = (ByteArrayList) postingList.getData();
            this.offset = 0;
        }
        
        @Override
        protected int nextNonNegativeIntIntern() {
            int result = 0;
            int shift = 0;
            do {
                final int b = data.getByte(offset);
                offset++;
                result += (b & 0x7F) << shift;
                if (b < 0) {
                    shift += 7;
                    assert shift<32;
                } else {
                    break;
                }
            } while (true);

            assert result >= 0;
            return result;
        }
        
        /** Moves to the next posting in the posting list and returns true if such a posting exists. Do not use
         * for the first posting. */
        
        @Override
        public boolean nextPosting() {
            if (offset >= data.size())
                return false;

            int b;
            do {
                b = this.nextNonNegativeIntIntern();
                //offset++;
                if (offset >= data.size())
                    return false;
            } while (b!=0);
            return true;
        }
    }
}