package mining.statistics;

public interface SPMLocalStatisticCollector {
	public void onObservedItem(int transactionId, int[] transaction, int position, int pFSTState);
}
