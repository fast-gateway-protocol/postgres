# FGP Postgres Daemon

## Doctrine

See [DOCTRINE.md](./DOCTRINE.md).


Fast PostgreSQL database operations via FGP protocol. Connect to any PostgreSQL database with connection pooling and low-latency queries.

## Quick Start

```bash
# Set connection via environment
export DATABASE_URL="postgres://user:pass@localhost:5432/mydb"

# Start daemon
fgp-postgres start

# Or run in foreground for debugging
fgp-postgres start -f

# Quick query (no daemon needed)
fgp-postgres query "SELECT NOW()"
```

## Installation

```bash
cargo install --path .

# Or build from source
cargo build --release
```

## Connection Configuration

### Environment Variables (recommended)

```bash
# Standard DATABASE_URL
export DATABASE_URL="postgres://user:pass@localhost:5432/mydb"

# Or libpq-style variables
export PGHOST=localhost
export PGPORT=5432
export PGUSER=myuser
export PGPASSWORD=mypass
export PGDATABASE=mydb
```

### Config File

Create `~/.fgp/auth/postgres/connections.json`:

```json
{
  "default": "local",
  "connections": {
    "local": {
      "host": "localhost",
      "port": 5432,
      "user": "postgres",
      "password": "secret",
      "database": "mydb"
    },
    "production": {
      "url": "postgres://user:pass@prod.example.com:5432/proddb?sslmode=require"
    }
  }
}
```

Then use named connections:

```bash
fgp-postgres start --connection production
fgp-postgres query "SELECT 1" --connection local
```

## CLI Commands

```bash
# Daemon management
fgp-postgres start           # Start daemon (background)
fgp-postgres start -f        # Start in foreground
fgp-postgres stop            # Stop daemon
fgp-postgres status          # Check daemon status

# Quick operations (no daemon)
fgp-postgres query "SELECT * FROM users LIMIT 5"
fgp-postgres tables                    # List tables
fgp-postgres tables --schema myschema  # Tables in specific schema
fgp-postgres connections               # List configured connections
```

## FGP Methods

| Method | Description | Parameters |
|--------|-------------|------------|
| `postgres.query` | Execute SELECT query | `sql` (required) |
| `postgres.execute` | Execute INSERT/UPDATE/DELETE | `sql` (required) |
| `postgres.transaction` | Execute statements in transaction | `statements[]` (required) |
| `postgres.tables` | List tables in schema | `schema` (default: "public") |
| `postgres.schema` | Get table schema | `table` (required), `schema` (default: "public") |
| `postgres.schemas` | List all schemas | - |
| `postgres.stats` | Database statistics | - |

## Examples

### Query

```bash
# Via socket (with running daemon)
echo '{"id":"1","v":1,"method":"query","params":{"sql":"SELECT * FROM users LIMIT 5"}}' \
  | nc -U ~/.fgp/services/postgres/daemon.sock

# Response
{
  "id": "1",
  "ok": true,
  "result": {
    "rows": [
      {"id": 1, "name": "Alice", "email": "alice@example.com"},
      {"id": 2, "name": "Bob", "email": "bob@example.com"}
    ],
    "row_count": 2,
    "columns": ["id", "name", "email"]
  }
}
```

### Transaction

```json
{
  "method": "postgres.transaction",
  "params": {
    "statements": [
      "UPDATE accounts SET balance = balance - 100 WHERE id = 1",
      "UPDATE accounts SET balance = balance + 100 WHERE id = 2",
      "INSERT INTO transfers (from_id, to_id, amount) VALUES (1, 2, 100)"
    ]
  }
}
```

### Get Table Schema

```json
{
  "method": "postgres.schema",
  "params": {
    "table": "users",
    "schema": "public"
  }
}
```

Response includes columns, constraints, and indexes.

## Performance

With connection pooling and warm connections:

| Operation | Cold Start | Warm (FGP) |
|-----------|------------|------------|
| Simple query | ~50-100ms | ~5-15ms |
| Complex query | ~100-500ms | ~50-200ms |
| Transaction | ~150-300ms | ~20-50ms |

## Socket Location

Default: `~/.fgp/services/postgres/daemon.sock`

Override with `--socket`:

```bash
fgp-postgres start --socket /tmp/my-postgres.sock
```

## Security Notes

- Passwords in `DATABASE_URL` or config files are stored in plaintext
- Consider using environment variables for production
- The daemon runs with the permissions of the user who started it
- SSL/TLS is supported via `sslmode` parameter in connection URL

## Troubleshooting

### Connection refused

```bash
# Check if PostgreSQL is running
pg_isready -h localhost -p 5432

# Check connection manually
psql postgres://user:pass@localhost:5432/mydb -c "SELECT 1"
```

### Permission denied on socket

```bash
# Check socket permissions
ls -la ~/.fgp/services/postgres/

# Remove stale socket
rm ~/.fgp/services/postgres/daemon.sock
```

### Daemon won't start

```bash
# Run in foreground to see errors
fgp-postgres start -f

# Check logs
RUST_LOG=debug fgp-postgres start -f
```
