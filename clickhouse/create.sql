DROP TABLE IF EXISTS otel_logs;

CREATE TABLE otel_logs
(
    -- Nanoseconds, the corpus's native precision (was DateTime = whole seconds,
    -- as in TextBench): 'recent' ORDER BY Timestamp then ranks the same rows as
    -- the other engines instead of breaking ties within a second arbitrarily.
    `Timestamp`          DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TraceId`            String CODEC(ZSTD(1)),
    `SpanId`             String CODEC(ZSTD(1)),
    `TraceFlags`         UInt8,
    `SeverityText`       LowCardinality(String) CODEC(ZSTD(1)),
    `SeverityNumber`     UInt8,
    `ServiceName`        LowCardinality(String) CODEC(ZSTD(1)),
    `Body`               String CODEC(ZSTD(1)),
    `ResourceSchemaUrl`  LowCardinality(String) CODEC(ZSTD(1)),
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeSchemaUrl`     LowCardinality(String) CODEC(ZSTD(1)),
    `ScopeName`          String CODEC(ZSTD(1)),
    `ScopeVersion`       LowCardinality(String) CODEC(ZSTD(1)),
    `ScopeAttributes`    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `LogAttributes`      Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    INDEX text_idx(Body) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(Body)__PHRASE_OPT__)
)
ENGINE = MergeTree
-- Sorting key from ./load's CH_ORDER_BY (default: upstream's (ServiceName, Timestamp)).
ORDER BY __ORDER_BY__
-- __PHRASE_SETTING__
;
