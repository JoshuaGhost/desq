package driver;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import mining.interestingness.DfsOnePassScored;
import mining.scores.DesqDfsScore;
import mining.scores.FrequencyScore;
import mining.scores.InformationGainScoreDesqCount;
import mining.scores.InformationGainScoreDesqCountMinSup;
import mining.scores.InformationGainScoreDesqCountNoPruning;
import mining.scores.InformationGainScoreDesqDFS;
import mining.scores.InformationGainScoreDesqDFSNoPruning;
import mining.scores.LocalInformationGainScore;
import mining.scores.NotImplementedExcepetion;
import mining.scores.RankedScoreList;
import mining.scores.RankedScoreListAll;
import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.statistics.old.GlobalInformationGainStatistic;
import mining.statistics.old.GlobalItemDocFrequencyStatistic;
import patex.PatEx;
import utils.Dictionary;
import visual.GraphViz;
import writer.SequentialWriter;

import com.google.common.base.Stopwatch;

import fst.Fst;
import fst.XFst;

/**
 * DesqDfsDriver.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class DesqDfsScoredDriver {

	// Timers
	public static Stopwatch totalTime = Stopwatch.createUnstarted();
	public static Stopwatch scanTime = Stopwatch.createUnstarted();
	public static Stopwatch fstTime = Stopwatch.createUnstarted();
	
	
	private static Logger logger = Logger.getLogger(DesqDfsScoredDriver.class.getSimpleName());
	
	public static void run(DesqConfig conf) throws Exception {
		String input = conf.getEncodedSequencesPath();
		String output = conf.getOutputSequencesPath();
		String patternExpression = conf.getPatternExpression();
		patternExpression = ".*[" + patternExpression.trim() + "]";
		double sigma = conf.getSigma();
		
		boolean writeOutput = conf.isWriteOutput();
		boolean useFlist = conf.isUseFlist();
	
		
		String sequenceFile = input.concat("/raw/part-r-00000");
		String dictionaryFile = input.concat("/wc/part-r-00000");
		
		
		/** load dictionary */
		Dictionary dict = Dictionary.getInstance();
		dict.load(dictionaryFile);
		
		/** initialize writer */
		if(writeOutput) {
		SequentialWriter writer = SequentialWriter.getInstance();
			writer.setItemIdToItemMap(dict.getItemIdToName());
			writer.setOutputPath(output);
		}	
			
		
		logger.log(Level.INFO, "Parsing pattern expression and generating FST");
		fstTime.start();
		
		PatEx ex = new PatEx(patternExpression);
		
		// Generate cFST
		Fst cFst = ex.translateToFst();
		cFst.minimize();
		
				
		// Generate optimized cFst
		XFst xFst = cFst.optimizeForExecution();
		
		logger.log(Level.INFO, "Took "+ fstTime.elapsed(TimeUnit.MILLISECONDS) + "ms");
		
		logger.log(Level.INFO, "Mining P-frequent sequences...");
		
		RankedScoreList rankedScoreList = new RankedScoreListAll(true);

		
		HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> globalDataCollectors = new HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>>();
		
		DesqDfsScore score = null;
		
		switch (conf.getScore()) {
		case SUPPORT:
			score = new FrequencyScore(xFst); 
			break;
		case VARINFGAIN:
			score = new LocalInformationGainScore(xFst); 
			break;
		case INFGAIN:
			score = new InformationGainScoreDesqDFS(xFst); 
			break;
		case INFGAINNOPRUNE:
			score = new InformationGainScoreDesqDFSNoPruning(xFst); 
			break;
		default:
			logger.severe("Interestingness measure is not supported for this mining method.");
			System.exit(0);
			break;
		}
		
		try {
			globalDataCollectors = score.getGlobalDataCollectors();
			
		} catch (NotImplementedExcepetion exception) {
			// do nothing
		}
		
		DfsOnePassScored dfs = new DfsOnePassScored(sigma, 
										xFst, 
										score, 
										rankedScoreList,
										globalDataCollectors,
										writeOutput);
		

		totalTime.start();
		
		scanTime.start();
		dfs.scan(sequenceFile);
		scanTime.stop();
		
		logger.log(Level.INFO, "Scan Took " + scanTime.elapsed(TimeUnit.SECONDS) +"s");
		
		dfs.mine();
		
		rankedScoreList.printList();

		totalTime.stop();

		logger.log(Level.INFO, "Total Took " + totalTime.elapsed(TimeUnit.SECONDS) +"s");
	}

}
