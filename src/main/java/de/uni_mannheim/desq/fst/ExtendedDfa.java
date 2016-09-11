package de.uni_mannheim.desq.fst;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.Item;


/**
 * ExtendedDfa.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public final class ExtendedDfa {
	

	//TODO: probably not needed
	//List<ExtendedDfaState> states = new ArrayList<>();
	Fst fst;
	Dictionary dict;
	
	// an index of eDfa stateIds for de.uni_mannheim.desq.old.fst stateIds
	ExtendedDfaState[] eDfaStateIdForFstStateId;
	
	
	public ExtendedDfa(Fst fst, Dictionary dict) {
		this(fst, dict, 1);
	}
	
	public ExtendedDfa(Fst fst, Dictionary dict, int numFstStates) {
		this.fst = fst;
		this.dict = dict;
		this.eDfaStateIdForFstStateId = new ExtendedDfaState[numFstStates];
		for(int fstStateId = 0; fstStateId < numFstStates; fstStateId++) {
			this.eDfaStateIdForFstStateId[fstStateId] = new ExtendedDfaState(fstStateId, fst);
		}
		this.constructExtendedDfa(fst);
	}
	
	/** Construct an extended-DFA from a given FST */
	// TODO: needs clean up
	private void constructExtendedDfa(Fst fst) {
		// Map old states to new state
		Map<IntSet, ExtendedDfaState> newStateForStateIdSet = new HashMap<>();
		
		// Map from item to reachable states
		Int2ObjectMap<IntSet> reachableStatesFromItemId = new Int2ObjectOpenHashMap<>(); 
		
		// Map reachable states to transitions
		Map<IntSet, BitSet> incTransitionToStates = new HashMap<>();
		
		// Unprocessed edfa states
		Stack<IntSet> unprocessedStateIdSets = new Stack<>();
		
		// processed edfa states
		Set<IntSet> processedStateIdSets = new HashSet<>();
		
		//helper
		List<Transition> transitionList = new ArrayList<>();
		
		
		// Initialize newStateForStateIdSet
		// Initially old states contain all de.uni_mannheim.desq.old.fst state as potential initial states
		for(int fstStateId = 0; fstStateId < eDfaStateIdForFstStateId.length; fstStateId++) {
			//ExtendedDfaState eDfaState = new ExtendedDfaState();
			IntSet initialStateIdSet = IntSets.singleton(fstStateId);
			newStateForStateIdSet.put(initialStateIdSet, eDfaStateIdForFstStateId[fstStateId]);
			//eDfaStateIdForFstStateId[fstStateId].setFstStates(initialStateIdSet, de.uni_mannheim.desq.old.fst);
			
			// add to unprocessed states
			unprocessedStateIdSets.push(initialStateIdSet);
			
			
		}
		
		while(!unprocessedStateIdSets.isEmpty()) {
			// process de.uni_mannheim.desq.old.fst states
			IntSet stateIdSet = unprocessedStateIdSets.pop();
			boolean isFinal = false;
			
			if(!processedStateIdSets.contains(stateIdSet)) {
				ExtendedDfaState fromEDfaState = newStateForStateIdSet.get(stateIdSet);
				
				reachableStatesFromItemId.clear();
				incTransitionToStates.clear();
				
				//TODO: optimize
				transitionList.clear();
				for(int stateId : stateIdSet) {
					transitionList.addAll(fst.getState(stateId).getTransitions());
					if(fst.getState(stateId).isFinal())
						isFinal = true;
				}
				
				//for all items, for all transitions
				for(Item item : dict.getItems()) {
					int itemFid = item.fid;
					
					for(Transition t : transitionList){
						if(t.matches(itemFid)) {
							IntSet reachableStates = reachableStatesFromItemId.get(itemFid);
							if(reachableStates == null) {
								reachableStates = new IntOpenHashSet();
								reachableStatesFromItemId.put(itemFid, reachableStates);
							}
							reachableStates.add(t.getToState().getId());
						}
					}
				}
				
				for(int itemFid : reachableStatesFromItemId.keySet()) {
					IntSet reachableStates = reachableStatesFromItemId.get(itemFid);
					BitSet eDfaTransition = incTransitionToStates.get(reachableStates);
					if(eDfaTransition == null) {
						eDfaTransition = new BitSet(dict.getItems().size() + 1);
						incTransitionToStates.put(reachableStates, eDfaTransition);
					}
					eDfaTransition.set(itemFid);
				}
				
				// Add transitions to extendedDfa
				for(IntSet reachableStateIds : incTransitionToStates.keySet()) {
					//check if we already processed it
					if(!processedStateIdSets.containsAll(reachableStateIds)) {
						unprocessedStateIdSets.add(reachableStateIds);
					}
					
					ExtendedDfaState toEDfaState = newStateForStateIdSet.get(reachableStateIds); 
					if(toEDfaState == null) {
						toEDfaState = new ExtendedDfaState(reachableStateIds, fst);
						newStateForStateIdSet.put(reachableStateIds, toEDfaState);
						//toEDfaState.setFstStates(reachableStateIds, de.uni_mannheim.desq.old.fst);
					}
					
					ExtendedDfaTransition eDfaTransition = 
							new ExtendedDfaTransition(incTransitionToStates.get(reachableStateIds), toEDfaState);
					fromEDfaState.addTransition(eDfaTransition);
				}
			}
			processedStateIdSets.add(stateIdSet);
			
			if(isFinal) {
				//newStateForStateIdSet.get(stateIdSet).isFinal = true;
			}
		}
		reachableStatesFromItemId.clear();
		incTransitionToStates.clear();
		processedStateIdSets.clear();
		//finalizeEDfa();
	}
	
	
	
	/*private IntSet createIntSet(int id) {
		IntSet initialStateIds = new IntOpenHashSet(1);
		initialStateIds.add(id);
		return initialStateIds;
	}*/

	
	/**
	 * Returns true if the de.uni_mannheim.desq.old.fst snapshot is relevant, i.e., leads to a final state
	 * otherwise returns false
	 */
	public boolean isRelevant(IntList inputSequence, int position, int fstStateId) {
		ExtendedDfaState state = eDfaStateIdForFstStateId[fstStateId];
		while(position < inputSequence.size()) {
			state = state.consume(inputSequence.getInt(position++));
			// In this case is ok to return false, if there was a final state before
			// we already retured true, final state can not be reached if state 
			// was null
			if(state == null)
				return false;
			if(state.isFinal())
				return true;
		}
		return false;
	}

	/**
	 * Returns true if the de.uni_mannheim.desq.old.fst snapshot is relevant, i.e., leads to a final state
	 * otherwise returns false
	 *
	 * Also adds to the given list with the sequence of states being visited before consuming each item + final one
	 * i.e., stateSeq[pos] = state before consuming inputSequence[pos]
	 */
	public boolean isRelevant(IntList inputSequence, int initialFstStateId, List<ExtendedDfaState> stateSeq,
							  IntList finalPos) {
		ExtendedDfaState state = eDfaStateIdForFstStateId[initialFstStateId];
		stateSeq.add(state);
		int pos = 0;
		while(pos < inputSequence.size()) {
			state = state.consume(inputSequence.getInt(pos++));
			if(state == null)
				break; // we may return true or false, as we might have reached a final state before
			stateSeq.add(state);
			if(state.isFinal())
				finalPos.add(pos);
		}
		return (!finalPos.isEmpty());
	}
	
	
	/**
	 * @param inputSequence
	 * @param initialFstStateId
	 * @param posStateIndex An array of bitsets; posStateIndex[pos].get(stateId) is true then stateId is reachable after cosuming intputSequence[pos]
	 * @param finalPos List of positions for which de.uni_mannheim.desq.old.fst reached a final state
	 * @return true is the input sequence has an accepting run 
	 */
	@Deprecated
	public boolean computeReachability(IntList inputSequence, int initialFstStateId, BitSet[] posStateIndex, IntList finalPos) {
		ExtendedDfaState state = eDfaStateIdForFstStateId[initialFstStateId];
		int pos = 0;
		while(pos < inputSequence.size()) {
			state = state.consume(inputSequence.getInt(pos));
			if(state == null)
				break; // we cannot return false here, as we might have reached a final state before
			posStateIndex[pos+1] = state.getFstStates();
			if(state.isFinal())
				finalPos.add(pos);
			pos++;
		}
		return (!finalPos.isEmpty());
	}

}
