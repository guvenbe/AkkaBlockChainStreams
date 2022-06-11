package streaming;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.stream.Attributes;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import blockchain.ManagerBehavior;
import blockchain.MiningSystemBehavior;
import model.Block;
import model.Transaction;
import utils.BlockChainUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class Main {

    private static int transId = -1;
    private static Random random = new Random();

    public static void main(String[] args) {
        ActorSystem<ManagerBehavior.Command> actorSystem= ActorSystem.create(MiningSystemBehavior.create(),
                "BlockChainMiner");

        Source<Transaction, NotUsed> transactionSource = Source.repeat(1)
                .throttle(1, Duration.ofSeconds(1))
                .map(x ->{
                    transId++;
                    System.out.println("Received transaction " + transId);
                    return new Transaction(transId, System.currentTimeMillis(), random.nextInt(1000),
                            random.nextDouble() * 100 );
                });
        Flow<Transaction, Block, NotUsed> blockBuilder = Flow.of(Transaction.class).map(trans->{
            List<Transaction> list = new ArrayList<>();
            list.add(trans);
            Block block = new Block("0", list);
            System.out.println("Created the block " + block);
            return block;
        }).conflate((block1, block2) ->{
            block1.addTransactionToList(block2.getFirstTransaction());
            System.out.println("Conflated the block" + block1);
            return block1;
        });
        Flow<Block, Block, NotUsed> miningProcess = Flow.of(Block.class).map( block ->{
            System.out.println("Starting to mine block " + block.toString());
            Thread.sleep(10000);
            System.out.println("Finished mining block " + block.toString());
        return block;
        });

        transactionSource
                .via(blockBuilder)
                .via(miningProcess.async().addAttributes(Attributes.inputBuffer(1,1)))
                .to(Sink.foreach(System.out::println))
                .run(actorSystem);
    }
}
