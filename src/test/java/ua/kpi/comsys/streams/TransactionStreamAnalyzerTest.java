package ua.kpi.comsys.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TransactionStreamAnalyzerTest {

    private static final LocalDateTime START = LocalDateTime.of(2025, 1, 1, 12, 0);

    private TransactionStreamAnalyzer analyzer;

    @BeforeEach
    void setUp() {
        analyzer = new TransactionStreamAnalyzer();
    }

    private static Transaction tx(int id, String amount) {
        return new Transaction("TX-" + id, "electronics", new BigDecimal(amount), START.plusMinutes(id));
    }

    private static Stream<Transaction> infiniteTransactions(AtomicInteger counter) {
        return Stream.iterate(1, i -> i + 1)
                .peek(i -> counter.incrementAndGet())
                .map(i -> tx(i, String.valueOf(i * 10)));
    }

    private static Stream<Transaction> infiniteConstant(String amount) {
        AtomicInteger ids = new AtomicInteger();
        return Stream.generate(() -> tx(ids.incrementAndGet(), amount));
    }

    @Nested
    class FindFirstHighValueTransaction {

        @Test
        void returnsFirstTransactionAboveThresholdInFiniteStream() {
            Stream<Transaction> stream = Stream.of(tx(1, "50"), tx(2, "150"), tx(3, "300"));

            Optional<Transaction> result = analyzer.findFirstHighValueTransaction(stream, new BigDecimal("100"));

            assertEquals(Optional.of("TX-2"), result.map(Transaction::transactionId));
        }

        @Test
        void treatsThresholdAsStrictLimit() {
            Stream<Transaction> stream = Stream.of(tx(1, "100.00"), tx(2, "100.01"));

            Optional<Transaction> result = analyzer.findFirstHighValueTransaction(stream, new BigDecimal("100"));

            assertEquals(Optional.of("TX-2"), result.map(Transaction::transactionId));
        }

        @Test
        void returnsEmptyWhenNothingExceedsThreshold() {
            Stream<Transaction> stream = Stream.of(tx(1, "10"), tx(2, "20"));

            assertTrue(analyzer.findFirstHighValueTransaction(stream, new BigDecimal("100")).isEmpty());
        }

        @Test
        void returnsEmptyForEmptyStream() {
            assertTrue(analyzer.findFirstHighValueTransaction(Stream.empty(), BigDecimal.ONE).isEmpty());
        }

        @Test
        void skipsNullTransactionsAndNullAmounts() {
            Transaction noAmount = new Transaction("TX-0", "books", null, START);
            Stream<Transaction> stream = Stream.of(null, noAmount, tx(1, "500"));

            Optional<Transaction> result = analyzer.findFirstHighValueTransaction(stream, new BigDecimal("100"));

            assertEquals(Optional.of("TX-1"), result.map(Transaction::transactionId));
        }

        @Test
        @Timeout(2)
        void stopsOnInfiniteStreamAfterFirstMatch() {
            AtomicInteger pulled = new AtomicInteger();

            Optional<Transaction> result =
                    analyzer.findFirstHighValueTransaction(infiniteTransactions(pulled), new BigDecimal("1000"));

            assertEquals(Optional.of("TX-101"), result.map(Transaction::transactionId));
            assertEquals(101, pulled.get());
        }

        @Test
        @Timeout(2)
        void findsMatchInGeneratedInfiniteStream() {
            AtomicInteger ids = new AtomicInteger();
            Stream<Transaction> stream = Stream.generate(() -> {
                int id = ids.incrementAndGet();
                return tx(id, id % 1000 == 0 ? "99999" : "1");
            });

            Optional<Transaction> result = analyzer.findFirstHighValueTransaction(stream, new BigDecimal("10000"));

            assertEquals(Optional.of("TX-1000"), result.map(Transaction::transactionId));
        }

        @Test
        void throwsWhenStreamIsNull() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.findFirstHighValueTransaction(null, BigDecimal.ONE));
        }

        @Test
        void throwsWhenThresholdIsNull() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.findFirstHighValueTransaction(Stream.empty(), null));
        }
    }

    @Nested
    class TakeTransactionsUntilBudgetExceeded {

        @Test
        void takesTransactionsWhileTotalFitsBudget() {
            Stream<Transaction> stream = Stream.of(tx(1, "30"), tx(2, "40"), tx(3, "50"), tx(4, "1"));

            List<Transaction> result =
                    analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100"));

            assertEquals(List.of("TX-1", "TX-2"), ids(result));
        }

        @Test
        void includesTransactionThatHitsBudgetExactly() {
            Stream<Transaction> stream = Stream.of(tx(1, "60"), tx(2, "40"), tx(3, "0.01"));

            List<Transaction> result =
                    analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100"));

            assertEquals(List.of("TX-1", "TX-2"), ids(result));
        }

        @Test
        void returnsEmptyListWhenFirstTransactionExceedsBudget() {
            Stream<Transaction> stream = Stream.of(tx(1, "500"), tx(2, "1"));

            assertTrue(analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100")).isEmpty());
        }

        @Test
        void returnsAllTransactionsOfFiniteStreamWithinBudget() {
            Stream<Transaction> stream = Stream.of(tx(1, "10"), tx(2, "20"));

            List<Transaction> result =
                    analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100"));

            assertEquals(List.of("TX-1", "TX-2"), ids(result));
        }

        @Test
        void allowsZeroBudget() {
            Stream<Transaction> stream = Stream.of(tx(1, "0"), tx(2, "0"), tx(3, "1"));

            List<Transaction> result = analyzer.takeTransactionsUntilBudgetExceeded(stream, BigDecimal.ZERO);

            assertEquals(List.of("TX-1", "TX-2"), ids(result));
        }

        @Test
        void skipsNullTransactionsAndNullAmounts() {
            Transaction noAmount = new Transaction("TX-0", "books", null, START);
            Stream<Transaction> stream = Stream.of(tx(1, "10"), null, noAmount, tx(2, "10"));

            List<Transaction> result = analyzer.takeTransactionsUntilBudgetExceeded(stream, BigDecimal.TEN);

            assertEquals(List.of("TX-1"), ids(result));
        }

        @Test
        void returnsUnmodifiableList() {
            List<Transaction> result =
                    analyzer.takeTransactionsUntilBudgetExceeded(Stream.of(tx(1, "1")), BigDecimal.TEN);

            assertThrows(UnsupportedOperationException.class, () -> result.add(tx(2, "1")));
        }

        @Test
        @Timeout(2)
        void stopsOnInfiniteIteratedStream() {
            AtomicInteger pulled = new AtomicInteger();

            List<Transaction> result =
                    analyzer.takeTransactionsUntilBudgetExceeded(infiniteTransactions(pulled), new BigDecimal("150"));

            assertEquals(List.of("TX-1", "TX-2", "TX-3", "TX-4", "TX-5"), ids(result));
            assertEquals(6, pulled.get());
        }

        @Test
        @Timeout(2)
        void stopsOnInfiniteGeneratedStream() {
            List<Transaction> result =
                    analyzer.takeTransactionsUntilBudgetExceeded(infiniteConstant("2.50"), new BigDecimal("1000"));

            assertEquals(400, result.size());
        }

        @Test
        void throwsWhenStreamIsNull() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.takeTransactionsUntilBudgetExceeded(null, BigDecimal.TEN));
        }

        @Test
        void throwsWhenBudgetIsNull() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.takeTransactionsUntilBudgetExceeded(Stream.empty(), null));
        }

        @Test
        void throwsWhenBudgetIsNegative() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.takeTransactionsUntilBudgetExceeded(Stream.empty(), new BigDecimal("-1")));
        }
    }

    @Nested
    class ProcessBatches {

        @Test
        void splitsFiniteStreamIntoFixedBatches() {
            Stream<Transaction> stream = Stream.of(tx(1, "1"), tx(2, "1"), tx(3, "1"), tx(4, "1"));

            List<List<Transaction>> result = analyzer.processBatches(stream, 2, 10);

            assertEquals(List.of(List.of("TX-1", "TX-2"), List.of("TX-3", "TX-4")), batchIds(result));
        }

        @Test
        void emitsTrailingPartialBatchOfFiniteStream() {
            Stream<Transaction> stream = Stream.of(tx(1, "1"), tx(2, "1"), tx(3, "1"));

            List<List<Transaction>> result = analyzer.processBatches(stream, 2, 10);

            assertEquals(List.of(List.of("TX-1", "TX-2"), List.of("TX-3")), batchIds(result));
        }

        @Test
        void doesNotEmitPartialBatchWhenLimitAlreadyReached() {
            Stream<Transaction> stream = Stream.of(tx(1, "1"), tx(2, "1"), tx(3, "1"));

            List<List<Transaction>> result = analyzer.processBatches(stream, 2, 1);

            assertEquals(List.of(List.of("TX-1", "TX-2")), batchIds(result));
        }

        @Test
        void returnsEmptyListForEmptyStream() {
            assertTrue(analyzer.processBatches(Stream.empty(), 3, 3).isEmpty());
        }

        @Test
        void skipsNullElements() {
            Stream<Transaction> stream = Stream.of(tx(1, "1"), null, tx(2, "1"));

            List<List<Transaction>> result = analyzer.processBatches(stream, 2, 1);

            assertEquals(List.of(List.of("TX-1", "TX-2")), batchIds(result));
        }

        @Test
        void returnsUnmodifiableBatches() {
            List<List<Transaction>> result = analyzer.processBatches(Stream.of(tx(1, "1")), 1, 1);

            assertThrows(UnsupportedOperationException.class, () -> result.getFirst().add(tx(2, "1")));
        }

        @Test
        @Timeout(2)
        void producesExactlyMaxBatchesFromInfiniteStream() {
            AtomicInteger pulled = new AtomicInteger();

            List<List<Transaction>> result = analyzer.processBatches(infiniteTransactions(pulled), 3, 2);

            assertEquals(List.of(List.of("TX-1", "TX-2", "TX-3"), List.of("TX-4", "TX-5", "TX-6")),
                    batchIds(result));
            assertEquals(6, pulled.get());
        }

        @Test
        @Timeout(2)
        void handlesLargeLimitsOnGeneratedStream() {
            List<List<Transaction>> result = analyzer.processBatches(infiniteConstant("1"), 100, 1000);

            assertEquals(1000, result.size());
            assertTrue(result.stream().allMatch(batch -> batch.size() == 100));
        }

        @Test
        @Timeout(2)
        void stopsWhenDownstreamShortCircuits() {
            AtomicInteger pulled = new AtomicInteger();

            Optional<List<Transaction>> first = infiniteTransactions(pulled)
                    .gather(BatchGatherer.<Transaction>of(4, Integer.MAX_VALUE))
                    .findFirst();

            assertEquals(Optional.of(4), first.map(List::size));
            assertEquals(4, pulled.get());
        }

        @Test
        void throwsWhenStreamIsNull() {
            assertThrows(IllegalArgumentException.class, () -> analyzer.processBatches(null, 1, 1));
        }

        @ParameterizedTest
        @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
        void throwsWhenBatchSizeIsNotPositive(int batchSize) {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.processBatches(infiniteConstant("1"), batchSize, 1));
        }

        @ParameterizedTest
        @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
        void throwsWhenMaxBatchesIsNotPositive(int maxBatches) {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.processBatches(infiniteConstant("1"), 1, maxBatches));
        }
    }

    private static List<String> ids(List<Transaction> transactions) {
        return transactions.stream().map(Transaction::transactionId).toList();
    }

    private static List<List<String>> batchIds(List<List<Transaction>> batches) {
        return batches.stream().map(TransactionStreamAnalyzerTest::ids).toList();
    }
}
