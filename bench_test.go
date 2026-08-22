package avro_test

import (
	"io"
	"os"
	"testing"

	"github.com/awaken/avro/v2"
)

type Superhero struct {
	ID            int           `avro:"id"`
	AffiliationID int           `avro:"affiliation_id"`
	Name          string        `avro:"name"`
	Life          float32       `avro:"life"`
	Energy        float32       `avro:"energy"`
	Powers        []*Superpower `avro:"powers"`
}

type Superpower struct {
	ID      int     `avro:"id"`
	Name    string  `avro:"name"`
	Damage  float32 `avro:"damage"`
	Energy  float32 `avro:"energy"`
	Passive bool    `avro:"passive"`
}

type PartialSuperhero struct {
	ID            int     `avro:"id"`
	AffiliationID int     `avro:"affiliation_id"`
	Name          string  `avro:"name"`
	Life          float32 `avro:"life"`
	Energy        float32 `avro:"energy"`
}

func BenchmarkSuperheroDecode(b *testing.B) {
	data, err := os.ReadFile("testdata/superhero.bin")
	if err != nil {
		panic(err)
	}

	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}

	super := &Superhero{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := avro.Unmarshal(schema, data, super); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSuperheroEncode(b *testing.B) {
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}

	super := &Superhero{
		ID:            234765,
		AffiliationID: 9867,
		Name:          "Wolverine",
		Life:          85.25,
		Energy:        32.75,
		Powers: []*Superpower{
			{ID: 2345, Name: "Bone Claws", Damage: 5, Energy: 1.15, Passive: false},
			{ID: 2346, Name: "Regeneration", Damage: -2, Energy: 0.55, Passive: true},
			{ID: 2347, Name: "Adamant skeleton", Damage: -10, Energy: 0, Passive: true},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := avro.Marshal(schema, super); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPartialSuperheroDecode(b *testing.B) {
	data, err := os.ReadFile("testdata/superhero.bin")
	if err != nil {
		panic(err)
	}

	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}

	super := &PartialSuperhero{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := avro.Unmarshal(schema, data, super); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSuperheroGenericDecode(b *testing.B) {
	data, err := os.ReadFile("testdata/superhero.bin")
	if err != nil {
		panic(err)
	}

	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var m any
		if err := avro.Unmarshal(schema, data, &m); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSuperheroGenericEncode(b *testing.B) {
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}

	super := map[string]any{
		"id":             234765,
		"affiliation_id": 9867,
		"name":           "Wolverine",
		"life":           float32(85.25),
		"energy":         float32(32.75),
		"powers": []map[string]any{
			{"id": 2345, "name": "Bone Claws", "damage": float32(5), "energy": float32(1.15), "passive": false},
			{"id": 2346, "name": "Regeneration", "damage": float32(-2), "energy": float32(0.55), "passive": true},
			{"id": 2347, "name": "Adamant skeleton", "damage": float32(-10), "energy": float32(0), "passive": true},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := avro.Marshal(schema, super); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSuperheroWriteFlush(b *testing.B) {
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}

	super := &Superhero{
		ID:            234765,
		AffiliationID: 9867,
		Name:          "Wolverine",
		Life:          85.25,
		Energy:        32.75,
		Powers: []*Superpower{
			{ID: 2345, Name: "Bone Claws", Damage: 5, Energy: 1.15, Passive: false},
			{ID: 2346, Name: "Regeneration", Damage: -2, Energy: 0.55, Passive: true},
			{ID: 2347, Name: "Adamant skeleton", Damage: -10, Energy: 0, Passive: true},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	w := avro.NewWriter(io.Discard, 128)
	for i := 0; i < b.N; i++ {
		w.WriteVal(schema, super)
		if err := w.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}
