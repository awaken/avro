package gen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"

	"github.com/awaken/avro/v2"
)

func (g *Generator) resetGraph() {
	g.roots = nil
	g.named = map[string]avro.NamedSchema{}
	g.checked = map[avro.NamedSchema]bool{}
	g.visited = map[avro.Schema]bool{}
	g.declared = map[string]bool{}
	g.symbols = map[string]string{}
}

func (g *Generator) claimName(symbols map[string]string, name, owner string) {
	if g.err != nil {
		return
	}
	if !ast.IsExported(name) {
		g.err = fmt.Errorf("%s normalizes to inaccessible Go identifier %q", owner, name)
		return
	}
	if previous, ok := symbols[name]; ok && previous != owner {
		g.err = fmt.Errorf("Go identifier %q collides between %s and %s", name, previous, owner)
		return
	}
	symbols[name] = owner
}

// Independent parses may contain copies of the same named definition. Accept
// equivalent output definitions, but never silently substitute a conflicting one.
func (g *Generator) registerNamed(schema avro.NamedSchema) bool {
	if g.checked[schema] {
		return true
	}
	previous := g.named[schema.FullName()]
	if previous == nil {
		g.named[schema.FullName()] = schema
		g.checked[schema] = true
		return true
	}
	if previous == schema {
		return true
	}
	a, err := g.schemaJSON(previous, map[string]bool{})
	if err != nil {
		g.err = err
		return false
	}
	b, err := g.schemaJSON(schema, map[string]bool{})
	if err != nil {
		g.err = err
		return false
	}
	if !bytes.Equal(a, b) {
		g.err = fmt.Errorf("conflicting Avro definitions for %q", schema.FullName())
		return false
	}
	g.checked[schema] = true
	return true
}

// A cycle consisting only of struct values has no finite Go representation.
// Nullable pointers, slices, maps and interface unions break that cycle.
func (g *Generator) checkValueCycles() error {
	definitions := make(map[string]typedef, len(g.typedefs))
	for _, definition := range g.typedefs {
		definitions[definition.Name] = definition
	}
	states := make(map[string]uint8, len(definitions))
	var visit func(string) error
	visit = func(name string) error {
		if states[name] == 1 {
			return fmt.Errorf("recursive Go value type %q requires nullable or collection indirection", name)
		}
		if states[name] == 2 {
			return nil
		}
		states[name] = 1
		for _, field := range definitions[name].Fields {
			if _, ok := definitions[field.Type]; ok {
				if err := visit(field.Type); err != nil {
					return err
				}
			}
		}
		states[name] = 2
		return nil
	}
	for _, definition := range g.typedefs {
		if err := visit(definition.Name); err != nil {
			return err
		}
	}
	return nil
}

func (g *Generator) schemaDefinitions() ([]string, error) {
	if !g.encoders || len(g.typedefs) == 0 {
		return nil, nil
	}
	seen := map[string]bool{}
	definitions := make([]string, 0, len(g.roots))
	for _, root := range g.roots {
		data, err := g.schemaJSON(root, seen)
		if err != nil {
			return nil, err
		}
		definitions = append(definitions, string(data))
	}
	return definitions, nil
}

// Expand a reference at its first occurrence and emit its full name thereafter.
// Each definition is therefore available before any reference back to it, even
// when a constructor supplied only RefSchema nodes or records are mutually recursive.
func (g *Generator) schemaJSON(schema avro.Schema, seen map[string]bool) ([]byte, error) {
	if ref, ok := schema.(*avro.RefSchema); ok {
		return g.schemaJSON(ref.Schema(), seen)
	}
	if named, ok := schema.(avro.NamedSchema); ok {
		if seen[named.FullName()] {
			return json.Marshal(named.FullName())
		}
		seen[named.FullName()] = true
	}
	var data []byte
	var err error
	if g.fullSchema {
		data, err = json.Marshal(schema)
	} else {
		data = []byte(avro.LegacyParsingCanonicalForm(schema))
	}
	if err != nil {
		return nil, err
	}
	if union, ok := schema.(*avro.UnionSchema); ok {
		types := union.Types()
		branches := make([]json.RawMessage, len(types))
		for i, branch := range types {
			branches[i], err = g.schemaJSON(branch, seen)
			if err != nil {
				return nil, err
			}
		}
		return json.Marshal(branches)
	}
	switch schema.(type) {
	case *avro.RecordSchema, *avro.ArraySchema, *avro.MapSchema:
	default:
		return data, nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	switch schema := schema.(type) {
	case *avro.RecordSchema:
		var fields []map[string]json.RawMessage
		if err := json.Unmarshal(object["fields"], &fields); err != nil {
			return nil, err
		}
		for i, field := range schema.Fields() {
			fields[i]["type"], err = g.schemaJSON(field.Type(), seen)
			if err != nil {
				return nil, err
			}
		}
		object["fields"], err = json.Marshal(fields)
	case *avro.ArraySchema:
		object["items"], err = g.schemaJSON(schema.Items(), seen)
	case *avro.MapSchema:
		object["values"], err = g.schemaJSON(schema.Values(), seen)
	}
	if err != nil {
		return nil, err
	}
	return json.Marshal(object)
}
