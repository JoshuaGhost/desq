/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.BitSet;

/**
 *
 * @author Kai
 */
public class EliasGammaPostingList extends AbstractPostingList{

    private int currentPosition;
    private BitSet data;
    
    private int freeBits;
    private int offset;
    private LongArrayList dataList;
    
    public EliasGammaPostingList() {
        // addInt()
        data = new BitSet();
        currentPosition = 0;
        
        // addInt2()
        dataList = new LongArrayList();
        dataList.add(0);
        offset = 0;
        freeBits = 64;
    }   
    
    //@Override
    public void addNonNegativeIntIntern2(int value) {
        
        int valueToAdd = value++;
        
        //System.out.println("valueToAdd: " + Integer.toBinaryString(valueToAdd));
        
        int positionBefore = currentPosition;
        
        byte startData = (byte) Integer.numberOfLeadingZeros(valueToAdd);
        
        int mask = 0x80000000 >>> startData;

        int length = 32 - startData;
        
        if(length >= 1){
            currentPosition += (length - 1);
            
            data.set(positionBefore, currentPosition, false);
        }
        
        while(mask != 0){
            if((mask & valueToAdd) != 0){
                data.set(++currentPosition, true);
            } else {
                data.set(++currentPosition, false);
            }
            
            mask >>>= 1;
        }

        currentPosition++;
    }

    @Override
    public void addNonNegativeIntIntern(int value){
        
        byte length = (byte) (32 - Integer.numberOfLeadingZeros(value));
        
        int totalLength = 2 * length - 1;
        
        if(freeBits >= totalLength){
            long tmp = dataList.getLong(dataList.size() - 1);
                        
            dataList.set(offset, tmp |= ((long) value) << (freeBits -= totalLength));
            
            if(freeBits == 0){
                freeBits = 64;
                offset++;
                dataList.add(0);
            }
        } else {
            if(length - 1 >= freeBits){
                int toAdd = (length - 1) - freeBits;
                freeBits = 64;
                offset++;
                
                if(toAdd == 0){
                    dataList.add(((long) value) << (freeBits -= length));
                } else {
                    dataList.add(((long) value) << (freeBits -= (length + toAdd)));
                }
            } else {
                long tmp = dataList.getLong(offset);
                
                int toAdd = totalLength - freeBits;
                dataList.set(offset, tmp |= ((long) value) >>> (totalLength - freeBits));
                
                freeBits = 64;
                offset++;
                
                dataList.add(((long) value) << (freeBits -= toAdd));
            }
        }  
    }

    @Override
    public int noBytes() {
        return this.dataList.size() * 8;
    }

    @Override
    public int size() {
        return this.dataList.size();
    }

    @Override
    public void clear() {
        this.dataList.clear();
    }

    @Override
    public void trim() {
        this.dataList.trim();
    }
        
    @Override
    public AbstractIterator iterator() {
        return new Iterator(this);
    }
    
    
    public void printData(){
        for(int i = 0; i < dataList.size(); i++){
            System.out.println("data: " + Long.toBinaryString(dataList.getLong(i)));
        }
    }
    
    private class Iterator extends AbstractIterator{

        private LongArrayList data;
        private long currentData;
        
        private byte internalOffset;
        
        public Iterator(EliasGammaPostingList postingList){
            this.data = postingList.dataList;
            
            assert this.data.size() > 0;
            this.currentData = this.data.getLong(offset);
            
            this.internalOffset = 0;
        }
        
        @Override
        int nextNonNegativeIntIntern() {
            byte leadingZeros = (byte)(Long.numberOfLeadingZeros(this.currentData) - this.internalOffset);
            byte length = (byte) ((leadingZeros * 2) + 1);
            
            int returnValue = 0;
            
            if((this.internalOffset += length) <= 64){
                returnValue = (int) (this.currentData >>> (64 - this.internalOffset));
                
                if(length == 64){
                    this.offset++;
                    this.currentData = this.data.getLong(offset);
                } else {
                    this.currentData &= ((long)1 << 64 - this.internalOffset) - 1;
                }
            } else {
                offset++;
                long tmp = this.data.getLong(offset);
                
                if((int)(this.currentData << (this.internalOffset - 64)) != 0){
                    this.internalOffset -= 64;
                    
                    returnValue = (int) (this.currentData << this.internalOffset | (tmp >>> 64 - this.internalOffset));

                    this.currentData = tmp & (((long)1 << 64 - this.internalOffset) - 1);
                } else {
                    byte dataLength = (byte)Long.numberOfLeadingZeros(tmp);

                    this.internalOffset = (byte)((dataLength + (dataLength + leadingZeros)) + 1);
                    
                    returnValue = (int)(tmp >>> (64 - this.internalOffset));
                    
                    this.currentData = tmp & (((long)1 << 64 - this.internalOffset) - 1);
                }
            }
            
            return returnValue;
        }
        
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
    
    public static void main(String[] args){
        EliasGammaPostingList postingList = new EliasGammaPostingList();
        
        postingList.newPosting();
        
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(512);
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(96);
        postingList.addNonNegativeInt(5);
        postingList.addNonNegativeInt(75);
        postingList.addNonNegativeInt(12);
        postingList.addNonNegativeInt(1963);
        postingList.addNonNegativeInt(9432);
        postingList.addNonNegativeInt(23);
        
        postingList.printData();
        
        AbstractIterator iterator = postingList.iterator();
        
        System.out.println("Next Int: " + iterator.nextNonNegativeInt());
        System.out.println("Next Int: " + iterator.nextNonNegativeInt());
        System.out.println("Next Int: " + iterator.nextNonNegativeInt());
        System.out.println("Next Int: " + iterator.nextNonNegativeInt());
        System.out.println("Next Int: " + iterator.nextNonNegativeInt());
        System.out.println("Next Int: " + iterator.nextNonNegativeInt());
        System.out.println("Next Int: " + iterator.nextNonNegativeInt());
        System.out.println("Next Int: " + iterator.nextNonNegativeInt());
        System.out.println("Next Int: " + iterator.nextNonNegativeInt());
        System.out.println("Next Int: " + iterator.nextNonNegativeInt());
        System.out.println("Next Int: " + iterator.nextNonNegativeInt());
        System.out.println("Next Int: " + iterator.nextNonNegativeInt());
        System.out.println("Next Int: " + iterator.nextNonNegativeInt());
        
    }
}
