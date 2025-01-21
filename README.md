# EUMETSAT Fetcher

```bash
# Cross-Compilation for linux/arm64
GOOS=linux GOARCH=arm64 go build -o eumetsat_fetcher main.go

./eumetsat_fetcher \
    --consumer-key "<consumer-key>" \
    --consumer-secret "<consumer-secret>" \
    --num-workers 10 \
    --input-file "<path-to-list-of-products>" \
    --output-dir "<path-to-output-dir>"
```
