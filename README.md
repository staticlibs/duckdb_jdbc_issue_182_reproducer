DuckDB JDBC issue reproducer
----------------------------

Run rerpoducer to [#182](https://github.com/duckdb/duckdb-java/issues/182) (adjust parquet path and memory setting inside the `.java` file):

```
java -XX:MaxRAM=128M -cp path/to/duckdb_jdbc.jar Issue182Reproducer.java
```

