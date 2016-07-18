package de.uni_mannheim.desq.mining;

import java.util.ArrayList;

import it.unimi.dsi.fastutil.ints.IntList;

public abstract class MemoryDesqMiner extends DesqMiner {
	protected ArrayList<int[]> inputSequences = new ArrayList<>();
	
	protected MemoryDesqMiner(DesqMinerContext ctx) {
		super(ctx);
	}
	
	@Override
	public void addInputSequence(IntList inputSequence) {
		inputSequences.add( inputSequence.toIntArray() );
	}
}
