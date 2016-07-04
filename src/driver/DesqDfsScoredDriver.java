package driver;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import mining.interestingness.DfsOnePassScored;
import mining.scores.FrequencyScore;
import mining.scores.InformationGainScore;
import mining.scores.LocalInformationGainScore;
import mining.scores.RankedScoreList;
import mining.scores.RankedScoreListAll;
import mining.scores.SPMScore;
import mining.statistics.GlobalInformationGainStatistic;
import mining.statistics.GlobalItemDocFrequencyStatistic;
import patex.PatEx;
import utils.Dictionary;
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
		GlobalItemDocFrequencyStatistic globalItemFrequency = new GlobalItemDocFrequencyStatistic();
		SPMScore score = new FrequencyScore(globalItemFrequency);
		
		
//		GlobalInformationGainStatistic globalInformationGainStatistic = new GlobalInformationGainStatistic(sequenceFile);
//		SPMScore score = new InformationGainScore(xFst.convertToFstGraph(), globalInformationGainStatistic, Dictionary.getInstance(), xFst);

//		SPMScore score = new LocalInformationGainScore();
		
		DfsOnePassScored dfs = new DfsOnePassScored(sigma, xFst, score, rankedScoreList, score.getLocalCollectors(), writeOutput);
		
		totalTime.start();
		
		dfs.scan(sequenceFile);
		dfs.mine();
		
		rankedScoreList.printList();

		totalTime.stop();

		logger.log(Level.INFO, "Took " + totalTime.elapsed(TimeUnit.SECONDS) +"s");
	}

}
