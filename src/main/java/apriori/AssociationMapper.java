package apriori;

import list.Association;
import list.ItemSet;
import list.Transaction;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import trie.Trie;
import utils.AprioriUtils;
import utils.AssociationRule;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//if error revert kmapper
public class AssociationMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text item = new Text();
    HashMap<ItemSet,Integer> itemSetsAll = new HashMap<>();

    @Override
    public void setup(Context context) {

        int passNum = context.getConfiguration().getInt("passNum", 2);
        String outDir=context.getConfiguration().get("outPrefix");
        for(int i=1;i<passNum;i++) {
            String lastPassOutputFile = "pass" + i + "/part-r-00000";

            // In try part, it reads the itemSet from the previous pass.
            try {
                Path path = new Path(outDir,lastPassOutputFile);
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(path)));
                String currLine;

                // Each line is shown in the following form : [frequent ItemSet] support
                // i.e [1, 2] 3
                //     [2, 3] 2
                // Therefore, We need to filter '[' , ']'

                while ((currLine = fis.readLine()) != null) {
                    currLine = currLine.replace("[", "");
                    currLine = currLine.replace("]", "");
                    currLine = currLine.trim();
                    String[] words = currLine.split("[\\s\\t]+");
                    if (words.length < 2) {
                        continue;
                    }

                    String finalWord = words[words.length - 1];
                    int support = Integer.parseInt(finalWord);
                    ItemSet itemSet = new ItemSet(support);

                    for (int k = 0; k < words.length - 1; k++) {
                        String csvItemIds = words[k];
                        String[] itemIds = csvItemIds.split(",");
                        for (String itemId : itemIds) {
                            itemSet.add(Integer.parseInt(itemId));
                        }
                    }
                    itemSetsAll.put(itemSet,support);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
    //Get all binary splits. Then calculate confidence and lift
    @Override
    public void map(LongWritable id, Text value, Context context)
            throws IOException, InterruptedException {
        ItemSet itemSet1=AprioriUtils.getItemSet(value.toString());
        Text key=new Text();
        Text val=new Text();
        int numTxn=context.getConfiguration().getInt("numTxns", 2); // Getting numTxns from configuration
        double minConf=Double.parseDouble(context.getConfiguration().get("minConf"));
        double minSup=Double.parseDouble(context.getConfiguration().get("minSup"));
        ArrayList<Association> rules=AprioriUtils.generateRulesFromItemSet(itemSet1,itemSetsAll,numTxn);
        for (Association rule :
                rules) {
            AssociationRule associationRule=new AssociationRule(rule.getSupport(numTxn),rule.getConfidence(),rule.getLift(numTxn),!(rule.isFirstNegated()|rule.isSecondNegated()));
            AprioriDriver.log.info(rule+" "+associationRule);
            if(associationRule.getSupport()>=minSup && associationRule.getConfidence()>=minConf && associationRule.getLift()>=1) {
                key.set(rule.toString());
                val.set(associationRule.toString());
                context.write(key, val);
                context.getCounter(AprioriDriver.File.LINES_WRITTEN).increment(1);
            }
        }
    }
}
