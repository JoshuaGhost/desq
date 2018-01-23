package de.uni_mannheim.desq.experiments.fimi;

import de.uni_mannheim.desq.examples.ExampleUtils;
import de.uni_mannheim.desq.mining.*;
import de.uni_mannheim.desq.util.DesqProperties;

import java.io.IOException;

public class EvaluatePerfOnFimi {

    public enum Miner {DesqCount, DesqDfs, DesqDfsPatricia, DesqDfsPatriciaIndex}

    private static final String retail_itemset_data = "data-local/fimi_retail/retail.dat";
    private static final String retail_itemset_dict = "data-local/fimi_retail/dict.json";
    private static final String retail_seqOfItemsets_data = "data-local/fimi_retail/retail_sequences.dat";
    private static final String retail_seqOfItemsets_dict = "data-local/fimi_retail/dict.json";
    private static final String click_itemset_data = "data-local/fimi/kosarak/kosarak.dat";//"data-local/fimi_click/kosarak.dat";
    private static final String click_seqOfItemsets_data = "data-local/fimi_click/kosarak_sequences.dat";

    private static DesqProperties getMinerConf(Miner miner, String patEx, long sigma){
        DesqProperties conf;
        switch (miner){
            case DesqCount:
                conf = DesqCount.createConf(patEx, sigma);
                conf.setProperty("desq.mining.use.two.pass", false); // has to be set for DesqCount due to bug
                break;
            case DesqDfs:
                conf = DesqDfs.createConf(patEx, sigma);
                conf.setProperty("desq.mining.use.two.pass", false); // has to be set for DesqDfs to return correct results
                break;
            case DesqDfsPatricia:
                conf = DesqDfsPatricia.createConf(patEx, sigma);
                conf.setProperty("desq.mining.use.two.pass", false); // not used anyways
                break;
            case DesqDfsPatriciaIndex:
                conf = DesqDfsPatriciaIndex.createConf(patEx, sigma);
                conf.setProperty("desq.mining.use.two.pass", false); // not used anyways
                break;
            default: throw new UnsupportedOperationException("Unsupported Miner");
        }

        conf.setProperty("desq.mining.prune.irrelevant.inputs", true);

        conf.setProperty("desq.mining.optimize.permutations", true);

        return conf;
    }

    private static void runSequentialFimi(Miner miner) throws IOException{

        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        "(-/){1,3} [(/) (-/){1,3}]{1,2}",
                        5000),
                click_seqOfItemsets_data,
                null,
                false,
                "/",
                "data-local/log/Fimi_Seq_" + miner + "_",
                1,
                50,
                false, false, true,
                true
        );
    }

    private static void runFimi(Miner miner) throws IOException{

        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        //"(.) (.)",
                        "AB^ (.){1,3}", //"A B 30 1198 (.)", //"(.) (.)", //"A B (.){1,3}", //"A B (.){1,5}"
                        100),
                retail_itemset_data, retail_itemset_dict,
                //click_itemset_data, null,
                false,
                null,
                "data-local/log/Fimi_" + miner + "_",
                3,
                0,
                false, true, false,
                true
        );
    }

    public static void runItemsetExample(Miner miner) throws IOException{
        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        "[c|d] / (B)",
                        1),
                "data/itemset-example/data.dat",
                "data/itemset-example/dict.json",
                false,
                "/",
                "data-local/log/ItemsetEx_" + miner + "_",
                10,
                10,
                true, true, true,
                true
        );
    }

    public static void runIcdm16(Miner miner) throws IOException{
        ExampleUtils.runItemsetPerfEval(
                getMinerConf(miner,
                        //"[c|d] (.){1,3} A",
                        //"(.){1,3}",
                        "(A^) (e)", //"(A^)",
                        1),
                "data/icdm16-example/data.del",
                "data/icdm16-example/dict.json",
                true,
                "/",
                "data-local/log/icdm16Ex_" + miner + "_",
                1,
                50,
                false, true, false,
                true
        );
    }

    public static void main(String[] args) throws IOException{
        //runItemsetExample(Miner.DesqCount);

        //runFimi(Miner.DesqCount);
        runFimi(Miner.DesqDfs);
        runFimi(Miner.DesqDfsPatricia);
        runFimi(Miner.DesqDfsPatriciaIndex);
        //runSequentialFimi(Miner.DesqDfs);


        //runIcdm16(Miner.DesqDfs);
        //runIcdm16(Miner.DesqDfsPatricia);

    }
}
