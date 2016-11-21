package de.uni_mannheim.desq.fst;

import java.util.*;
import java.util.stream.Collectors;


public final class State {
	
	int id;
	// List of transitions
	List<Transition> transitionList;
	boolean isFinal;
	
	public State() {
		this(false);
	}
	
	
	public State(boolean isFinal) {
		this.transitionList = new ArrayList<>();
		this.isFinal = isFinal;
	}
	
	
	public int getId(){
		return id;
	}
	
	public void setId(int id){
		this.id = id;
	}
	
	public void addTransition(Transition t) {
		BasicTransition tr = (BasicTransition) t;
		tr.addFromState(this);
		transitionList.add(t);
	}
	
	public void simulateEpsilonTransitionTo(State to) {
		if (to.isFinal)
			isFinal = true;
		for (Transition t : to.transitionList) {
			transitionList.add(t);
		}
	}
	
	private static final class TransitionIterator implements Iterator<ItemState> {
		Iterator<Transition> transitionsIt;
		Iterator<ItemState> currentIt;
		BitSet validToStates;
		int fid;
        boolean isNew;

		@Override
		public boolean hasNext() {
			if (currentIt == null || isNew) {
				Transition nextTransition = nextTransition();
				if (nextTransition != null) {
					currentIt = nextTransition.consume(fid, currentIt);
                    isNew = false;
				} else {
					return false;
				}
			}
			while (!currentIt.hasNext()) {
				Transition nextTransition = nextTransition();
				if (nextTransition != null) {
					currentIt = nextTransition.consume(fid, currentIt);
				} else {
					return false;
				}
			}
			return true;
		}

		@Override
		public ItemState next() {
			return currentIt.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		private Transition nextTransition() {
			if (validToStates==null) {
				if (transitionsIt.hasNext()) {
					return transitionsIt.next();
				} else {
					return null;
				}
			} else {
				while (transitionsIt.hasNext()) {
					Transition nextTransition = transitionsIt.next();
					if (validToStates.get(nextTransition.toState.getId())) {
						return nextTransition;
					}
				}
				return null;
			}
		}
	}

	private static final class CompressedTransitionIterator implements Iterator<Transition> {
		Iterator<Transition> transitionsIt;
		//Iterator<ItemState> currentIt;
		Transition nextTransition;
		BitSet validToStates;
		int fid;
		boolean isNew;

		@Override
		public boolean hasNext() {
			do {
				nextTransition = nextTransition();
				if(nextTransition == null) {
					return false;
				}
			} while(!nextTransition.matches(fid));
			return true;
			/*
			if (currentIt == null || isNew) {
				Transition nextTransition = nextTransition();
				if (nextTransition != null) {
					currentIt = nextTransition.consume(fid, currentIt);
					isNew = false;
				} else {
					return false;
				}
			}
			while (!currentIt.hasNext()) {
				Transition nextTransition = nextTransition();
				if (nextTransition != null) {
					currentIt = nextTransition.consume(fid, currentIt);
				} else {
					return false;
				}
			}
			return true;*/
		}

		@Override
		public Transition next() {
			return nextTransition;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		private Transition nextTransition() {
			if (validToStates==null) {
				if (transitionsIt.hasNext()) {
					return transitionsIt.next();
				} else {
					return null;
				}
			} else {
				while (transitionsIt.hasNext()) {
					Transition nextTransition = transitionsIt.next();
					if (validToStates.get(nextTransition.toState.getId())) {
						return nextTransition;
					}
				}
				return null;
			}
		}
	}
	
	public Iterator<ItemState> consume(int itemFid) {
		return consume(itemFid, null, null);
	}
	
	public Iterator<ItemState> consume(int itemFid, Iterator<ItemState> it) {
		return consume(itemFid, it, null);
	}

	/** Returns an iterator over (output item, next state)-pairs consistent with the given input item. Only
	 * produces pairs for which the next state is contained in validToStates (BitSet indexed by state ids).
	 *
	 * If the output item is epsilon, returns (0, next state) pair.
	 *
	 * @param itemFid input item
	 * @param it iterator to reuse
	 * @param validToStates set of next states to consider
	 *
	 * @return an iterator over (output item fid, next state) pairs
	 */
	public Iterator<ItemState> consume(int itemFid, Iterator<ItemState> it, BitSet validToStates) {
		TransitionIterator resultIt;
		if(it != null && it instanceof TransitionIterator)
			resultIt = (TransitionIterator)it;
		else
			resultIt = new TransitionIterator();

		resultIt.transitionsIt = transitionList.iterator();
		resultIt.validToStates = validToStates;
		resultIt.fid = itemFid;
		resultIt.isNew = true;

		return resultIt;
	}


	public Iterator<Transition> consumeCompressed(int itemFid) {
		return consumeCompressed(itemFid, null, null);
	}

	public Iterator<Transition> consumeCompressed(int itemFid, Iterator<Transition> it) {
		return consumeCompressed(itemFid, it, null);
	}

	/** Returns an iterator over (output item, next state)-pairs consistent with the given input item. Only
	 * produces pairs for which the next state is contained in validToStates (BitSet indexed by state ids).
	 *
	 * If the output item is epsilon, returns (0, next state) pair.
	 *
	 * @param itemFid input item
	 * @param it iterator to reuse
	 * @param validToStates set of next states to consider
	 *
	 * @return an iterator over (output item fid, next state) pairs
	 */
	public Iterator<Transition> consumeCompressed(int itemFid, Iterator<Transition> it, BitSet validToStates) {
		CompressedTransitionIterator resultIt;
		if(it != null && it instanceof CompressedTransitionIterator)
			resultIt = (CompressedTransitionIterator)it;
		else
			resultIt = new CompressedTransitionIterator();

		resultIt.transitionsIt = transitionList.iterator();
		resultIt.validToStates = validToStates;
		resultIt.fid = itemFid;
		resultIt.isNew = true;

		return resultIt;
	}

	public boolean isFinal() { 
		return isFinal; 
	}

	public List<Transition> getTransitions() {
		return transitionList;
	}

	public List<BasicTransition> getSortedBasicTransitions() {
		// quick & easy. to be improved
		List<BasicTransition> basicTransitionList = transitionList.stream().map(e -> (BasicTransition) e).collect(Collectors.toList());
		basicTransitionList.sort(Comparator.comparing((BasicTransition t)->t.outputLabelType)
			.thenComparing(t->t.outputLabel)
			.thenComparing(t->t.inputLabelType)
			.thenComparing(t->t.inputLabel));
		// now that we have sorted them, store them in this order
		transitionList = basicTransitionList.stream().map(e -> (Transition) e).collect(Collectors.toList());
		return basicTransitionList;
	}
	
	private static class StateIterator implements Iterator<State> {
		Iterator<Transition> transitionsIt;
		Transition transition;
		int fid;
		State toState;
		BitSet toStatesOutput = new BitSet(); // to states already output
		@Override
		public boolean hasNext() {
			while(transitionsIt.hasNext()) {
				transition = transitionsIt.next();
				if(transition.matches(fid) &&  !toStatesOutput.get(transition.toState.id)) { // returns false if toStatesOutput is to small
					toState = transition.toState; 
					toStatesOutput.set(toState.id); // automatically resizes upwards if necessary
					return true;
				}
			}
			toState = null;
			return false;
		}

		@Override
		public State next() {
			return toState;
		}
		
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}


	
	public Iterator<State> toStateIterator(int itemFid) {
		return toStateIterator(itemFid, null);
	}
	
	public Iterator<State> toStateIterator(int itemFid, Iterator<State> it) {
		StateIterator resultIt ;
		if(it != null && it instanceof StateIterator) 
			resultIt = (StateIterator) it;
		else
			resultIt = new StateIterator();
		
		resultIt.transitionsIt = transitionList.iterator();
		resultIt.fid = itemFid;
		resultIt.toStatesOutput.clear();
		resultIt.toState = null;
		return resultIt;
	}
}
