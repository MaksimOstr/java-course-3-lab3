# Лабораторна робота №3

**Дисципліна:** Технології розробки на платформі Java
**Тема:** Опрацювання нескінченних послідовностей даних за допомогою ледачих (lazy) операцій Java Stream API, безпечне керування наявністю значень через `Optional` та створення кастомних проміжних операцій за допомогою Stream Gatherers.

## Опис проєкту

Java-проєкт, що збирається за допомогою Gradle (Groovy DSL) із керуванням залежностями через Version Catalog (`gradle/libs.versions.toml`). Використовується Java 25 (toolchain), оскільки `java.util.stream.Gatherer` є стабільним API починаючи з Java 24.

Усі класи знаходяться в пакеті `ua.kpi.comsys.streams`:

| Клас | Призначення |
|------|-------------|
| `Transaction` | Модель транзакції: `transactionId`, `category`, `amount`, `timestamp` |
| `TransactionStreamAnalyzer` | Сервіс аналітики стріму транзакцій |
| `BatchGatherer` | Фабрика кастомного stateful `Gatherer` (через `Gatherer.ofSequential`), що групує елементи у батчі фіксованого розміру |

### Методи `TransactionStreamAnalyzer`

| Метод | Поведінка |
|-------|-----------|
| `Optional<Transaction> findFirstHighValueTransaction(Stream<Transaction> stream, BigDecimal threshold)` | Повертає першу транзакцію з `amount > threshold` (`filter` + `findFirst`). Якщо такої немає у скінченному стрімі — `Optional.empty()` |
| `List<Transaction> takeTransactionsUntilBudgetExceeded(Stream<Transaction> stream, BigDecimal maxBudget)` | Накопичує транзакції, поки їхня сумарна вартість не перевищує `maxBudget`. Накопичена сума рахується через `Gatherers.scan`, обрізання стріму — через `takeWhile` |
| `List<List<Transaction>> processBatches(Stream<Transaction> stream, int batchSize, int maxBatches)` | Розбиває стрім на батчі по `batchSize` елементів за допомогою `BatchGatherer` і зупиняє обробку після формування рівно `maxBatches` батчів |

### Правила обробки

- якщо `stream` або обов'язковий аргумент дорівнює `null`, кидається `IllegalArgumentException`;
- якщо `maxBudget < 0`, кидається `IllegalArgumentException`;
- якщо `batchSize <= 0` або `maxBatches <= 0`, кидається `IllegalArgumentException`;
- `null`-елементи стріму та транзакції без `amount` ігноруються;
- межа `threshold` строга (`amount > threshold`), а бюджет включний (сума `<= maxBudget`);
- для скінченного стріму останній неповний батч також повертається, якщо ліміт `maxBatches` ще не досягнуто;
- усі повернені списки незмінні.

### Робота з нескінченними стрімами

- `findFirstHighValueTransaction` використовує операцію короткого замикання `findFirst`;
- `takeTransactionsUntilBudgetExceeded` використовує `takeWhile`, тому обробка припиняється на першій транзакції, що виводить суму за межі бюджету;
- `BatchGatherer` повертає `false` з інтегратора після формування `maxBatches` батчів і враховує `downstream.isRejecting()`, тому джерело перестає генерувати елементи (з нескінченного стріму зчитується рівно `batchSize * maxBatches` елементів).

## Розрахунок варіанта

Номер залікової книжки: **N = 21**

```
V = N mod 3 = 21 mod 3 = 0
```

**Варіант 0 — Електронна комерція та стрім транзакцій.**

## Структура проєкту

```
.
├── build.gradle
├── settings.gradle
├── gradle/
│   ├── libs.versions.toml
│   └── wrapper/
├── src/
│   ├── main/java/ua/kpi/comsys/streams/
│   │   ├── BatchGatherer.java
│   │   ├── Transaction.java
│   │   └── TransactionStreamAnalyzer.java
│   └── test/java/ua/kpi/comsys/streams/
│       └── TransactionStreamAnalyzerTest.java
└── README.md
```

## Запуск тестів

Потрібна JDK 25.

Linux / macOS:

```bash
./gradlew test
```

Windows:

```bat
gradlew.bat test
```

Тести використовують нескінченні джерела `Stream.iterate(...)` та `Stream.generate(...)` і позначені `@Timeout(2)`, що підтверджує відсутність зависань. Додатково перевіряється кількість елементів, фактично зчитаних із джерела.

Після виконання тестів звіт про покриття JaCoCo доступний у `build/reports/jacoco/test/html/index.html`, а звіт тестів — у `build/reports/tests/test/index.html`.
