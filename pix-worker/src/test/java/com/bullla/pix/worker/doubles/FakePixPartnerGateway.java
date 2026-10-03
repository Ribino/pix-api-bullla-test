package com.bullla.pix.worker.doubles;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import com.bullla.pix.worker.domain.gateway.pix.PartnerResult;
import com.bullla.pix.worker.domain.gateway.pix.PixPartnerGateway;
import com.bullla.pix.worker.domain.model.pix.PixTransaction;

public class FakePixPartnerGateway implements PixPartnerGateway {

    private final Supplier<PartnerResult> behavior;
    private final List<String> processedTransactionIds = new ArrayList<>();

    public FakePixPartnerGateway(PartnerResult fixedResult) {
        this(() -> fixedResult);
    }

    public FakePixPartnerGateway(Supplier<PartnerResult> behavior) {
        this.behavior = behavior;
    }

    public static FakePixPartnerGateway throwing(RuntimeException failure) {
        return new FakePixPartnerGateway(() -> {
            throw failure;
        });
    }

    @Override
    public PartnerResult process(PixTransaction transaction) {
        processedTransactionIds.add(transaction.transactionId());
        return behavior.get();
    }

    public int processedCount() {
        return processedTransactionIds.size();
    }
}
