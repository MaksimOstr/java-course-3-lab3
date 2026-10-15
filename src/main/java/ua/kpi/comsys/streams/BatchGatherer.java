package ua.kpi.comsys.streams;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Gatherer;

final class BatchGatherer {

    private BatchGatherer() {
    }

    private static final class State<T> {
        private List<T> current = new ArrayList<>();
        private int emitted;
    }

    static <T> Gatherer<T, ?, List<T>> of(int batchSize, int maxBatches) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        if (maxBatches <= 0) {
            throw new IllegalArgumentException("maxBatches must be greater than 0");
        }
        return Gatherer.<T, State<T>, List<T>>ofSequential(
                State::new,
                Gatherer.Integrator.of((state, element, downstream) -> {
                    state.current.add(element);
                    if (state.current.size() < batchSize) {
                        return !downstream.isRejecting();
                    }
                    List<T> batch = List.copyOf(state.current);
                    state.current = new ArrayList<>();
                    state.emitted++;
                    return downstream.push(batch) && state.emitted < maxBatches;
                }),
                (state, downstream) -> {
                    if (!state.current.isEmpty() && state.emitted < maxBatches && !downstream.isRejecting()) {
                        state.emitted++;
                        downstream.push(List.copyOf(state.current));
                    }
                });
    }
}
