.PHONY: local-up local-down local-trail

local-up: data
	docker-compose up -d

local-down:
	docker-compose down

local-logs:
	docker-compose logs -f dynamodb

clean:
	git clean -fXd