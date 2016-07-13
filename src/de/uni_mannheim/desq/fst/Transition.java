package de.uni_mannheim.desq.fst;

import java.util.Iterator;

public abstract class Transition {
	State toState;
	
	public boolean matches(int item) {
		return consume(item).hasNext();
	}
	
	public abstract Iterator<ItemState> consume(int item);
	public abstract Iterator<ItemState> consume(int item, Iterator<ItemState> it);
	
	// new transitions that can share all data but toStateId
	public abstract Transition shallowCopy();

	// setToState
	// getToState
}
