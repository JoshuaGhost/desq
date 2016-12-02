package de.uni_mannheim.desq.fst;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.RestrictedDictionary;
import it.unimi.dsi.fastutil.ints.*;

import java.util.Iterator;

public final class BasicTransition extends Transition {
	public enum InputLabelType {SELF, SELF_DESCENDANTS}

	public enum OutputLabelType {SELF, SELF_ASCENDANTS, CONSTANT, EPSILON}

	// parameters
	final int inputLabel;
	final InputLabelType inputLabelType;
	final int outputLabel; // only when outputlabel = constant or self_gen
	final OutputLabelType outputLabelType;
	final String inputLabelSid;
	final String outputLabelSid;

	// internal indexes
	final IntSet inputFids;
	final Dictionary outputDict;
	final boolean isForest;
	private State fromState;

	// variables for distributing DesqDfs
	private int transitionNumber = -1;

	private BasicTransition(BasicTransition other) {
		this.inputLabel = other.inputLabel;
		this.inputLabelType = other.inputLabelType;
		this.outputLabel = other.outputLabel;
		this.outputLabelType = other.outputLabelType;
		this.inputLabelSid = other.inputLabelSid;
		this.outputLabelSid = other.outputLabelSid;
		this.inputFids = other.inputFids;
		this.outputDict = other.outputDict;
		this.isForest = other.isForest;
	}

	// */* (no dots)
	public BasicTransition(int inputLabel, InputLabelType inputLabelType,
						   int outputLabel, OutputLabelType outputLabelType,
						   State toState, Dictionary dict) {
		this.inputLabel = inputLabel;
		this.inputLabelType = inputLabelType;
		this.outputLabel = outputLabel;
		this.outputLabelType = outputLabelType;
		this.inputLabelSid = inputLabel > 0 ? dict.sidOfFid(inputLabel): ".";
		this.outputLabelSid = outputLabel > 0 ? dict.sidOfFid(outputLabel) : null;
		this.toState = toState;

		if (inputLabel == 0)
			inputFids = null;
		else switch (inputLabelType) {
			case SELF:
				inputFids = IntSets.singleton(inputLabel);
				break;
			case SELF_DESCENDANTS:
				inputFids = dict.descendantsFids(inputLabel);
				break;
			default: // unreachable
				inputFids = null;
		}

		isForest = dict.isForest();
		if (outputLabelType == OutputLabelType.SELF_ASCENDANTS) {
			if (outputLabel == 0)
				outputDict = dict;
			else
				outputDict = new RestrictedDictionary(dict, dict.descendantsFids(outputLabel));
		} else outputDict = null;
	}


	private static final class ItemStateIterator implements Iterator<ItemState> {
		BasicTransition transition;
		int nextFid;
		final ItemState itemState = new ItemState();
		IntIterator fidIterator;
		final IntCollection ascendants; // reusable; fidIterator goes over this set

		ItemStateIterator(boolean isForest) {
			ascendants = isForest ? new IntArrayList() : new IntAVLTreeSet();
		}

		@Override
		public boolean hasNext() {
			return nextFid >= 0;
		}

		@Override
		public ItemState next() {
			assert nextFid >= 0;

			switch (transition.outputLabelType) {
				case EPSILON:
					itemState.itemFid = 0;
					nextFid = -1;
					break;
				case CONSTANT:
					itemState.itemFid = transition.outputLabel;
					nextFid = -1;
					break;
				case SELF:
					itemState.itemFid = nextFid;
					nextFid = -1;
					break;
				case SELF_ASCENDANTS:
					itemState.itemFid = nextFid;
					if (fidIterator.hasNext())
						nextFid = fidIterator.nextInt();
					else
						nextFid = -1;
			}

			return itemState;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	@Override
	public boolean matches(int itemFid) {
		return inputLabel == 0 || inputFids.contains(itemFid);
	}

	public boolean hasOutput() {
		return !outputLabelType.equals(OutputLabelType.EPSILON);
	}

	@Override
	public Iterator<ItemState> consume(int itemFid, Iterator<ItemState> it) {
		ItemStateIterator resultIt;
		if (it != null && it instanceof ItemStateIterator)
			resultIt = (ItemStateIterator) it;
		else
			resultIt = new ItemStateIterator(isForest);

		if (inputLabel == 0 || inputFids.contains(itemFid)) {
			resultIt.transition = this;
			resultIt.nextFid = itemFid;
			resultIt.itemState.state = toState;
			if (outputLabelType == OutputLabelType.SELF_ASCENDANTS) {
				resultIt.ascendants.clear();
				outputDict.addAscendantFids(itemFid, resultIt.ascendants);
				resultIt.fidIterator = resultIt.ascendants.iterator();
			}
		} else {
			resultIt.nextFid = -1;
		}

		return resultIt;
	}

	@Override
	public Transition shallowCopy() {
		return new BasicTransition(this);
	}

	@Override
	public String toPatternExpression() {
		String patternExpression;
		if (inputLabel == 0)
			patternExpression = ".";
		else {
			patternExpression = "\"" + inputLabelSid + "\"";
			if (inputLabelType == InputLabelType.SELF)
				patternExpression += "=";
		}
		switch (outputLabelType) {
			case SELF:
				patternExpression = "(" + patternExpression + ")";
				break;
			case SELF_ASCENDANTS:
				patternExpression = "(" + patternExpression + "^" + ")";
				break;
			case CONSTANT:
				patternExpression = "(" + patternExpression + "=^" + ")";
				break;
			case EPSILON:
				break;

		}
		return patternExpression;
	}

	@Override
	public String labelString() {
		StringBuilder sb = new StringBuilder();
        sb.append("T" + transitionNumber + (outputLabelType == OutputLabelType.SELF_ASCENDANTS ? "^" : "") + " ");
		if (inputLabel == 0)
			sb.append(".");
		else {
			sb.append(inputLabelSid);
			if (inputLabelType == InputLabelType.SELF)
				sb.append("=");
		}
		sb.append(":");
		switch (outputLabelType) {
			case SELF:
				sb.append("$");
				break;
			case SELF_ASCENDANTS:
				sb.append("$-" + (outputLabelSid != null ? outputLabelSid : ""));
				break;
			case CONSTANT:
				sb.append(outputLabelSid);
				break;
			case EPSILON:
				sb.append("ε");

		}
		return sb.toString();
	}

	@Override
	public String toString() {
		return labelString() + " -> " + toState.id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + inputLabel;
		result = prime * result + ((inputLabelType == null) ? 0 : inputLabelType.hashCode());
		result = prime * result + outputLabel;
		result = prime * result + ((outputLabelType == null) ? 0 : outputLabelType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BasicTransition other = (BasicTransition) obj;
		if (inputLabel != other.inputLabel)
			return false;
		if (inputLabelType != other.inputLabelType)
			return false;
		if (outputLabel != other.outputLabel)
			return false;
		return outputLabelType == other.outputLabelType;
	}

	public void getInputFids(IntSet inputFids) {
		if (this.inputFids == null)
			inputFids.add(0);
		else
			inputFids.addAll(this.inputFids);
	}

	@Override
	public boolean isDotEps() {
//		if(inputLabel == 0 && outputLabelType == OutputLabelType.EPSILON)
//			return true;
//		else
//			return false;
//
		return (inputLabel == 0 && outputLabelType == OutputLabelType.EPSILON);
	}
	

	/**
	 * For partition constrution in DesqDfs
	 */
	public OutputLabelType getOutputLabelType() { return outputLabelType; }
	public State getToState() { return toState; }
	public int getOutputLabel() { return outputLabel; }
	public void addAscendantFids(int itemFid, IntCollection ascendants) { outputDict.addAscendantFids(itemFid, ascendants); }
	public void setTransitionNumber(int number) {
		this.transitionNumber = number;
	}
	public int getTransitionNumber() { return transitionNumber; }

	/**
	 * Returns a list of output elements produced by this transition, given the passed input item
	 *
	 * NFA encoding uses this method only for SELF_ASCENDANT transitions,
	 * concat encoding also for SELF and CONSTANT
	 * @param inputItem
	 * @return
	 */
    public IntList getOutputElements(int inputItem) {
        IntList outputElements = new IntArrayList();
        // retrieve input item
        if(this.outputLabelType == OutputLabelType.CONSTANT) { // CONSTANT
            outputElements.add(getOutputLabel());
        } else if(this.outputLabelType == OutputLabelType.SELF) { // SELF
            outputElements.add(inputItem);
        } else if(this.outputLabelType == OutputLabelType.SELF_ASCENDANTS) { // SELF-ASCENDANTS
            // retrieve ascendants of the input item
            outputElements.add(inputItem);
            addAscendantFids(inputItem, outputElements);
        } else {
            System.out.println("CALLED getOutputElements for EPS-Transition!");
        }
        return outputElements;
    }

    public void addFromState(State state) {
		fromState = state;
	}
	public State getFromState() {
		return fromState;
	}

	/**
	 * Generalizes a given input item until one of two conditions is reached:
	 *   1) the current generalized item has 0 or more than 1 parent
	 *   2) the current generalized item is <= the pivot item
	 * @param itemFid
	 * @param pivotItem
	 * @return
	 */
	public int generalizeItemForPivot(int itemFid, int pivotItem) {
		IntArrayList parents;
		while(itemFid> pivotItem) {
			parents = outputDict.parentsOf(itemFid);
			if(parents.size() != 1)  // if the item has no parents or more than 1, we stop generalizing
				break;
			else
				itemFid = parents.getInt(0);
		}
		return itemFid ;
	}
}