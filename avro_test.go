package avro_test

import "github.com/awaken/avro/v2"

func ConfigTeardown() {
	// Reset the caches
	avro.DefaultConfig = avro.Config{}.Freeze()
}
