package de.uni_mannheim.desq.mining;

import org.apache.commons.configuration2.Configuration;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Created by rgemulla on 16.7.2016.
 */
@RunWith(Parameterized.class)
public class CSpadeMinerIcdm16Test extends Icdm16TraditionalMiningTest {
    public CSpadeMinerIcdm16Test(long sigma, int gamma, int lambda, boolean generalize) {
        super(sigma, gamma, lambda, generalize);
    }

    @Override
    public Configuration createConf() {
        return CSpadeMiner.createConf(sigma, gamma, lambda, generalize);
    }
}
