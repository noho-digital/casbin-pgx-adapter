.PHONY: test-db-up test-db-down test-db-reset test test-with-db clean

# Start test database
test-db-up:
	docker-compose -f docker-compose.yml up -d
	@echo "Waiting for database to be ready..."
	@until docker-compose -f docker-compose.yml exec postgres-test pg_isready -U postgres -d casbin_test; do \
		echo "Database not ready, waiting 2 seconds..."; \
		sleep 2; \
	done
	@echo "Test database is ready!"

# Stop test database
test-db-down:
	docker-compose -f docker-compose.yml down

# Reset test database (stop, remove volumes, start)
test-db-reset:
	docker-compose -f docker-compose.yml down -v
	docker-compose -f docker-compose.yml up -d
	@echo "Waiting for database to be ready..."
	@until docker-compose -f docker-compose.yml exec postgres-test pg_isready -U postgres -d casbin_test; do \
		echo "Database not ready, waiting 2 seconds..."; \
		sleep 2; \
	done
	@echo "Test database reset complete!"

# Run tests (assumes database is already running)
test:
	TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5433/casbin_test?sslmode=disable" go test -v ./...

# Connect to test database
test-db-connect:
	docker-compose -f docker-compose.yml exec postgres-test psql -U postgres -d casbin_test