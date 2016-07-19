package de.uni_mannheim.desq.mining;

import de.uni_mannheim.desq.util.PropertiesUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Properties;

/**
 * Created by rgemulla on 16.7.2016.
 */
@RunWith(Parameterized.class)
public class DesqDfsIcdm16Test extends Icdm16TraditionalMiningTest {
    public DesqDfsIcdm16Test(long sigma, int gamma, int lambda, boolean generalize) {
        super(sigma, gamma, lambda, generalize);
    }

    @Override
    public Class<? extends DesqMiner> getMinerClass() {
        return DesqDfs.class;
    }

    @Override
    public Properties createProperties() {
        Properties properties = new Properties();
        PropertiesUtils.set(properties, "minSupport", sigma);
        PropertiesUtils.set(properties, "patternExpression", DesqMiner.patternExpressionFor(gamma, lambda, generalize));
        return properties;
    }
}
