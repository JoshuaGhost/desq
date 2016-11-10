package de.uni_mannheim.desq.fst;

import java.util.BitSet;

/**
 * ExtendedDfaTransition.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
@Deprecated
public final class ExtendedDfaTransition {

	BitSet inputFids;
	ExtendedDfaState toState;
	
	public ExtendedDfaTransition(BitSet inputFids, ExtendedDfaState toState) {
		this.inputFids = inputFids;
		this.toState = toState;
	}
	
	public ExtendedDfaState getToState() {
		return toState;
	}
	
	
	/*public State getToStateId() {
		return toState.id;
	}*/
	
	public boolean matches(int itemFid) {
		return inputFids.get(itemFid);
	}
}
