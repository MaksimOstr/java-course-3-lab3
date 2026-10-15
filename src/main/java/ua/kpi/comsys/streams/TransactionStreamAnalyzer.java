package ua.kpi.comsys.streams;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Gatherers;
import java.util.stream.Stream;

public class TransactionStreamAnalyzer {

    private record RunningTotal(Transaction transaction, BigDecimal total) {}

    public Optional<Transaction> findFirstHighValueTransaction(Stream<Transaction> stream, BigDecimal threshold) {
        requireNonNull(stream, "stream");
        requireNonNull(threshold, "threshold");
        return stream
                .filter(transaction -> amountOf(transaction)
                        .filter(amount -> amount.compareTo(threshold) > 0)
                        .isPresent())
                .findFirst();
    }

    public List<Transaction> takeTransactionsUntilBudgetExceeded(Stream<Transaction> stream, BigDecimal maxBudget) {
        requireNonNull(stream, "stream");
        requireNonNull(maxBudget, "maxBudget");
        if (maxBudget.signum() < 0) {
            throw new IllegalArgumentException("maxBudget must not be negative");
        }
        return stream
                .filter(transaction -> amountOf(transaction).isPresent())
                .gather(Gatherers.scan(
                        () -> new RunningTotal(null, BigDecimal.ZERO),
                        (running, transaction) -> new RunningTotal(
                                transaction, running.total().add(transaction.amount()))))
                .takeWhile(running -> running.total().compareTo(maxBudget) <= 0)
                .map(RunningTotal::transaction)
                .toList();
    }

    public List<List<Transaction>> processBatches(Stream<Transaction> stream, int batchSize, int maxBatches) {
        requireNonNull(stream, "stream");
        return stream
                .filter(Objects::nonNull)
                .gather(BatchGatherer.<Transaction>of(batchSize, maxBatches))
                .toList();
    }

    private static Optional<BigDecimal> amountOf(Transaction transaction) {
        return Optional.ofNullable(transaction).map(Transaction::amount);
    }

    private static void requireNonNull(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
    }
}
