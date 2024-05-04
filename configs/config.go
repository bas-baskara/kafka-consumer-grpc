package configs

import (
	"log"

	"github.com/caarlos0/env/v8"
	"github.com/joho/godotenv"
)

type config struct {
	GRPC_DIAL_OPTION string `env:"GRPC_DIAL_OPTION"`
	CERT_FILE_PATH string `env:"CERT_FILE_PATH"`

	KAFKA_BROKER             string `env:"KAFKA_BROKER"`
	KAFKA_VERSION            string `env:"KAFKA_VERSION"`
	KAFKA_WORKER_POOL_NUMBER string `env:"KAFKA_WORKER_POOL_NUMBER"`

	GRPC_ADDRESS string `env:"GRPC_ADDRESS"`
}

func EnvGetter() *config {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Failed to load env file", err)
		return nil
	}

	cfg := new(config)
	if err := env.Parse(cfg); err != nil {
		log.Fatal("Failed to parse env", err)
		return nil
	}

	return cfg
}
