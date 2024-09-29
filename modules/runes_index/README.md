
## rune-index

```
mkdir -p /ndata/mainnet/psql
mkdir -p /ndata/mainnet/rune-index-data/ord-folder
```

```
1. use db_init.sql
psql -h localhost -p 5432 -U postgres -f db_init.sql
 
2. 
INSERT INTO runes_network_type (network_type) VALUES ('mainnet');
```

## txs-index

```
node index_block.js
```