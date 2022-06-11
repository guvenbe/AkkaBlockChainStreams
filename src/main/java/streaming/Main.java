package streaming;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.stream.Attributes;
import akka.stream.FanInShape2;
import akka.stream.FlowShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.*;
import akka.stream.typed.javadsl.ActorFlow;
import blockchain.ManagerBehavior;
import blockchain.MiningSystemBehavior;
import model.Block;
import model.HashResult;
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
        ActorSystem<ManagerBehavior.Command> actorSystem = ActorSystem.create(MiningSystemBehavior.create(),
                "BlockChainMiner");

        Source<Transaction, NotUsed> transactionSource = Source.repeat(1)
                .throttle(1, Duration.ofSeconds(1))
                .map(x -> {
                    transId++;
                    System.out.println("Received transaction " + transId);
                    return new Transaction(transId, System.currentTimeMillis(), random.nextInt(1000),
                            random.nextDouble() * 100);
                });
        Flow<Transaction, Block, NotUsed> blockBuilder = Flow.of(Transaction.class).map(trans -> {
            List<Transaction> list = new ArrayList<>();
            list.add(trans);
            Block block = new Block("0", list);
            System.out.println("Created the block " + block);
            return block;
        }).conflate((block1, block2) -> {
            block1.addTransactionToList(block2.getFirstTransaction());
            System.out.println("Conflated the block" + block1);
            return block1;
        });
        Flow<Block, HashResult, NotUsed> miningProcess = ActorFlow
                .ask(actorSystem, Duration.ofSeconds(30), (block, self) ->
                        new ManagerBehavior.MineBlockCommand(block, self, 5)
                );

        Flow<Block, Block, NotUsed> miningFlow = Flow.fromGraph(
                GraphDSL.create(builder -> {
                    UniformFanOutShape<Block, Block> broadcast = builder.add(Broadcast.create(2));
                    FlowShape<Block, HashResult> mineBlock = builder.add(miningProcess);
                    //partial graph
                    FanInShape2<Block,HashResult,Block> receiveHashResult =
                            builder.add(ZipWith.create((block, hashResult) ->{
                                block.setHash(hashResult.getHash());
                                block.setNonce(hashResult.getNonce());
                                return block;
                            }));
                    builder.from(broadcast)
                            .toInlet(receiveHashResult.in0());
                    builder.from(broadcast)
                            .via(mineBlock)
                            .toInlet(receiveHashResult.in1());
                    return FlowShape.of(broadcast.in(), receiveHashResult.out());
                })
        );


        transactionSource
                .via(blockBuilder)
                .via(miningFlow.async().addAttributes(Attributes.inputBuffer(1, 1)))
                .to(Sink.foreach(System.out::println))
                .run(actorSystem);
    }
}
