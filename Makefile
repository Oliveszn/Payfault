.PHONY: run migrate test curl-pay curl-status

run:
	go run ./cmd/server/...

migrate:
	psql $$(DATABASE_URL) -f migrations/001_init.sql

test:
	go test ./... -v

curl-pay:
	curl -s -X POST http://localhost:8080/pay \
	  -H "Content-Type: application/json" \
	  -d "{\"amount\":100000,\"recipient_code\":\"RCP_xxxxxxxxxxxx\",\"sender_ref\":\"user_001\"}" \
	  | jq .
# 	curl -s -X POST http://localhost:8080/pay \
# 	  -H "Content-Type: application/json" \
# 	  -d '{"amount": 100000, "recipient_code": "RCP_xxxxxxxxxxxx", "sender_ref": "user_001"}' \
# 	  | jq .

curl-status:
	curl -s http://localhost:8080/transaction/$(TXN_ID) | jq .

	curl-pay:
