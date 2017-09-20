package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.longs.LongArrayList;

/**
 *
 * @author Kai
 */
public class VarByteLongPostingList extends AbstractPostingList{
    
    private final LongArrayList data;
    private final LongArrayList control;
        
    private int dataOffset;
    private int controlOffset;
    
    private long currentData;
    private long currentControl;
    
    private boolean wroteData;
    private boolean wroteControl;
    
    /** Sets up a new empty posting list. */
    public VarByteLongPostingList(){   
        this.data = new LongArrayList();
        this.control = new LongArrayList();
        
        this.clear();
    }
    
    @Override
    protected void addNonNegativeIntIntern(int value) {
        
        int dataCount = 0;
        
        // Get the number of bytes (bits to simplify shifting).
        if((value >>> 8) == 0) {dataCount = 8;}
        else if((value >>> 16) == 0) {dataCount = 16;}
        else if((value >>> 24) == 0) {dataCount = 24;}
        else {dataCount = 32;}
        
        int freeBits = 64 - this.dataOffset;
        
        // Check if the integer value fits in the current long value.
        if(freeBits >= dataCount){            
            currentData |= ((long)value << this.dataOffset);
            
            this.dataOffset += dataCount;
            
            if(dataOffset == 64){
                if(this.wroteData){
                    this.data.set(this.data.size() - 1, currentData);
                    this.wroteData = false;
                } else {
                    this.data.add(currentData);
                }
                
                this.currentData = 0;
                this.dataOffset = 0;
            }
        } else {
            if(this.wroteData){
                this.data.set(this.data.size() - 1, currentData | ((long)value << this.dataOffset));
                this.wroteData = false;
            } else {
                this.data.add(currentData | ((long)value << this.dataOffset));
            }
            
            this.dataOffset = (dataCount - freeBits);
            currentData = (long)value >>> freeBits;
        }

        // Set the two control data bits
        if(dataCount > 8)
            this.currentControl |= ((dataCount >>> 3L) - 1L) << controlOffset;
        
        controlOffset += 2;
        
        if(controlOffset == 64){
            if(this.wroteControl){
                this.control.set(this.control.size() - 1, currentControl);
                this.wroteControl = false;
            } else {
                this.control.add(currentControl);
            }
            
            this.currentControl = 0;
            this.controlOffset = 0;
        }
    }

    @Override
    public void newPosting(){
        noPostings++;
        if (noPostings>1) // first posting does not need separator            
            this.addNonNegativeIntIntern(0);
    }
    
    @Override
    public void clear() {
        this.data.clear();
        this.control.clear();
        
        this.wroteData = false;
        this.wroteControl = false;
        
        this.currentControl = 0;
        this.currentData = 0;
                
        this.controlOffset = 0;
        this.dataOffset = 0;
        
        this.noPostings = 0;
    }

    @Override
    public int noBytes() {
        return this.data.size() * 8 + this.control.size() * 8;
    }

    @Override
    public void trim() {
        this.data.trim();
        this.control.trim();
    }

    @Override
    public AbstractIterator iterator() {
        return new Iterator(this);
    }
    
    public static final class Iterator extends AbstractIterator{

        private LongArrayList data;
        private LongArrayList control;
        
        private long currentData;
        private long currentControl;
        
        private int dataOffset;
        private int controlOffset;
        
        private int internalDataOffset;
        private int internalControlOffset;
        
        private int noPostings;
        private int count;
        
        /** Sets up a new empty iterator. */
        public Iterator(){
            this.data = null;
            this.control = null;
            
            this.dataOffset = 0;
            this.controlOffset = 0;
            this.internalDataOffset = 0;
            this.internalControlOffset = 0;
            
            this.currentData = 0;
            this.currentControl = 0;
            
            this.count = 1;
        }
        
        /** Sets up an iterator for the given posting list. */
        public Iterator(VarByteLongPostingList postingList){
            this.data = postingList.data;
            this.control = postingList.control;
            
            if(postingList.wroteData){
                this.data.set(this.data.size() - 1, postingList.currentData);
            } else {
                this.data.add(postingList.currentData);
                postingList.wroteData = true;
            }
            
            if(postingList.wroteControl){
                this.control.set(this.control.size() - 1, postingList.currentControl);
            } else {
                this.control.add(postingList.currentControl);
                postingList.wroteControl = true;
            }
            
            this.noPostings = postingList.noPostings;
            
            this.reset();
        }
        
        /** Resets the iterator. */
        public void reset(){
            this.dataOffset = 0;
            this.controlOffset = 0;
            this.internalDataOffset = 0;
            this.internalControlOffset = 0;
            
            this.currentData = this.data.getLong(dataOffset);
            this.currentControl = this.control.getLong(controlOffset);

            this.count = 1;
        }
        
        @Override
        public void reset(AbstractPostingList postingList) {
            VarByteLongPostingList postingListTmp = (VarByteLongPostingList) postingList;
            
            this.data = postingListTmp.data;
            this.control = postingListTmp.control;
            
            if(postingListTmp.wroteData){
                this.data.set(this.data.size() - 1, postingListTmp.currentData);
            } else {
                this.data.add(postingListTmp.currentData);
                postingListTmp.wroteData = true;
            }
            
            if(postingListTmp.wroteControl){
                this.control.set(this.control.size() - 1, postingListTmp.currentControl);
            } else {
                this.control.add(postingListTmp.currentControl);
                postingListTmp.wroteControl = true;
            }
            
            this.noPostings = postingListTmp.noPostings;
            
            this.reset();
        }
        
        @Override
        int nextNonNegativeIntIntern() {
            // Get the number of bytes (bits to simplify shifting) to read.
            int dataCount = (int) (((currentControl) & 3) + 1) * 8;
            
            int possibleBits = 64 - internalControlOffset;
            
            int returnValue = (int) ((this.currentData & ((1L << dataCount) - 1)));
            
            // Check if the integer value is partiioned onto two long values.
            if(dataCount <= possibleBits){
                this.currentData >>>= dataCount;
                this.internalControlOffset += dataCount;
            } else {
                this.dataOffset++;
                this.currentData = this.data.getLong(this.dataOffset);

                returnValue |= (int) ((this.currentData & ((1 << (dataCount - possibleBits)) - 1))) << possibleBits;

                this.internalControlOffset = dataCount - possibleBits;
                
                this.currentData >>>= this.internalControlOffset;
            }
            
            if(this.internalControlOffset == 64){
                this.internalControlOffset = 0;
                this.dataOffset++;
                this.currentData = this.data.getLong(this.dataOffset);
            }
            
            this.internalDataOffset += 2;
            this.currentControl >>= 2;
            
            if(this.internalDataOffset == 64){
                this.internalDataOffset = 0;
                this.controlOffset++;
                this.currentControl = this.control.getLong(this.controlOffset);
            }
            
            return returnValue;
        }

        @Override
        public boolean nextPosting() {
            if (dataOffset > data.size() || count >= noPostings)
                return false;

            int b;
            do {
                b = this.nextNonNegativeIntIntern();
            } while (b!=0);
            count++;
            return true;
        }

        @Override
        public boolean hasNext() {
            return dataOffset < data.size() && !((currentControl & 3) == 0 && (currentData & 0xFF) == 0);
        }
    }
}
