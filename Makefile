.PHONY: run test build build-web up infra down

# Runs the processor with the built-in device simulator. No broker, no database.
run:
	cd processor-java && mvn -B spring-boot:run

test:
	cd processor-java && mvn -B test

build:
	cd processor-java && mvn -B package

build-web:
	cd dashboard-ts && npm install && npm run build

up:
	docker compose up --build

# Adds Mosquitto (MQTT) and MongoDB
infra:
	docker compose --profile infra up --build

down:
	docker compose down
