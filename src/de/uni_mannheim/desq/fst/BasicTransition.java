package de.uni_mannheim.desq.fst;

import java.util.Iterator;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.Itemset;
import de.uni_mannheim.desq.dictionary.SingleItemset;
import it.unimi.dsi.fastutil.ints.IntIterator;

public class BasicTransition extends Transition {
	enum InputLabelType { SELF, SELF_DESCENDANTS };
	enum OutputLabelType { SELF, SELF_ASCENDANTS, CONSTANT, EPSILON };

	// parameters
	final int inputLabel; 
	final InputLabelType inputLabelType;
	final int outputLabel; // only when outputlabel = constant or self_gen
	final OutputLabelType outputLabelType;

	// internal indexes
	final Itemset inputFids;
	final Dictionary outputDict;

	private BasicTransition(BasicTransition other) {
		this.inputLabel = other.inputLabel;
		this.inputLabelType = other.inputLabelType;
		this.outputLabel = other.outputLabel;
		this.outputLabelType = other.outputLabelType;
		this.inputFids = other.inputFids;
		this.outputDict = other.outputDict;
	}
	
	// */* (no dots)
	public BasicTransition(int inputLabel, InputLabelType inputLabelType, 
			int outputLabel, OutputLabelType outputLabelType, 
			State toState, Dictionary dict) {
		this.inputLabel = inputLabel;
		this.inputLabelType = inputLabelType;
		this.outputLabel = outputLabel;
		this.outputLabelType = outputLabelType;
		this.toState = toState;

		if (inputLabel == 0)
			inputFids = null;
		else switch (inputLabelType) {
		case SELF:
			inputFids = Itemset.create(inputLabel);
			break;
		case SELF_DESCENDANTS:
			inputFids = dict.descendantsItemset(inputLabel);
			break;
		default:
			 inputFids = null;
		}
		
		if (outputLabelType == OutputLabelType.SELF_ASCENDANTS) {
			if (outputLabel == 0) 
				outputDict = dict;
			else
				outputDict = dict.descendantsDictionary(outputLabel);
		} else outputDict = null;
	}	

	
	private class ItemStateIterator implements Iterator<ItemState> {
		int fid;
		ItemState itemState;
		IntIterator fidIterator;
		
		@Override
		public boolean hasNext() {
			return fid>=0;
		}

		@Override
		public ItemState next() {
			assert fid>=0;
			
			switch (outputLabelType) {
			case EPSILON:
				itemState.itemFid = 0;
				fid = -1;
				break;
			case CONSTANT:
				itemState.itemFid = outputLabel;
				fid = -1;
				break;
			case SELF:
				itemState.itemFid = fid;
				fid = -1;
				break;
			case SELF_ASCENDANTS:
				itemState.itemFid = fid;
				if (fidIterator.hasNext())
					fid = fidIterator.next().intValue();
				else
					fid = -1;
			}

			return itemState;
		}
		
	}
	
	@Override
	public boolean matches(int itemFid) {
		return inputLabel==0 || inputFids.contains(itemFid);
	}
	
	@Override
	public Iterator<ItemState> consume(int itemFid) {
		return consume(itemFid, null);
	}

	@Override
	public Iterator<ItemState> consume(int itemFid, Iterator<ItemState> it) {
		ItemStateIterator it2 = null;
		if (it != null && it instanceof ItemStateIterator) 
			it2 = (ItemStateIterator)it;
		else
			it2 = new ItemStateIterator();
		
		if (inputLabel==0 || inputFids.contains(itemFid)) {
			it2.itemState.itemFid = itemFid;
			it2.itemState.state = toState;
			if (outputLabelType == OutputLabelType.SELF_ASCENDANTS) {
				it2.fidIterator = outputDict.ascendantsIterator(itemFid);
			}
		} else {
			it2.itemState.itemFid = -1;
		}

		return it2;
	}

	@Override
	public Transition shallowCopy() {
		return new BasicTransition(this);
	}

}
