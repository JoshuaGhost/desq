/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Kai-Arne
 */
public class PostingListTest {
    
    public AbstractPostingList postingList;
    public AbstractIterator iterator;
    public int numberOfElements;
    public int[] inputData;
    
    @Before
    public void setUp(){
        postingList = new NewPostingList();
        numberOfElements = 100000000;
        inputData = new int[numberOfElements];
                 
        for(int i = 0; i < numberOfElements; i++){
            int prop = (int) (Math.random() * 100);
             
            if(i < 5){
                inputData[i] = 0;
            } else if (i >= 5 && i < 10){
                inputData[i] = (int) (Math.random() * 2000000000);
            } else if (i >= 20){
                inputData[i] = (int) (Math.random() * 127);
            }
        }
        
        postingList.newPosting();
        
        for(int i = 0; i < numberOfElements; i++){
            postingList.addNonNegativeInt(inputData[i]);
        }
        
        iterator = postingList.iterator();
    }
    
    @Test
    public void postingListTest(){        
        for(int i = 0; i < numberOfElements; i++){
            assertEquals(inputData[i], iterator.nextNonNegativeInt());
        }
    }
}
