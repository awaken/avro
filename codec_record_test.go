package avro

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

type recordValue struct {
	Value int32 `avro:"value" json:"value" yaml:"value" xml:"value"`
}
type recordLeft struct{ recordValue }
type recordRight struct{ recordValue }
type recordDiamond struct {
	recordLeft
	recordRight
	Tail int32 `avro:"tail" json:"tail" yaml:"tail" xml:"tail"`
}
type recordDiamondReversed struct {
	recordRight
	recordLeft
	Tail int32 `avro:"tail" json:"tail" yaml:"tail" xml:"tail"`
}

func TestRecordAmbiguousEmbedding(t *testing.T) {
	schema := MustParse(`{"type":"record","name":"Ambiguous","fields":[{"name":"value","type":"int"},{"name":"tail","type":"int"}]}`)
	api := Config{}.Freeze()
	data, err := api.Marshal(schema, map[string]any{"value": int32(7), "tail": int32(9)})
	if err != nil {
		t.Fatal(err)
	}
	left := recordLeft{recordValue{Value: 1}}
	right := recordRight{recordValue{Value: 2}}
	for _, value := range []any{recordDiamond{recordLeft: left, recordRight: right}, recordDiamondReversed{recordLeft: left, recordRight: right}} {
		if _, err := api.Marshal(schema, value); err == nil || !strings.Contains(err.Error(), "missing required field") {
			t.Errorf("ambiguous %T encoded without a missing-field error: %v", value, err)
		}
	}
	first := recordDiamond{recordLeft: left, recordRight: right}
	second := recordDiamondReversed{recordLeft: left, recordRight: right}
	if err := api.Unmarshal(schema, data, &first); err != nil {
		t.Fatal(err)
	}
	if err := api.Unmarshal(schema, data, &second); err != nil {
		t.Fatal(err)
	}
	if first.recordLeft.Value != 1 || first.recordRight.Value != 2 || first.Tail != 9 ||
		second.recordLeft.Value != 1 || second.recordRight.Value != 2 || second.Tail != 9 {
		t.Fatal("decoding changed an ambiguous field or lost the following field")
	}
}

func TestRecordTaggedFieldDominance(t *testing.T) {
	schema := MustParse(`{"type":"record","name":"Tagged","fields":[{"name":"Value","type":"int"}]}`)
	api := Config{}.Freeze()
	value := struct {
		Value int32 `json:"value" yaml:"value" xml:"value"`
		Other int32 `avro:"Value" json:"other" yaml:"other" xml:"other"`
	}{Value: 1, Other: 2}
	data, err := api.Marshal(schema, value)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, []byte{4}) {
		t.Errorf("tagged field lost to an untagged peer: %x", data)
	}
	data, err = api.Marshal(schema, map[string]any{"Value": int32(7)})
	if err != nil {
		t.Fatal(err)
	}
	if err := api.Unmarshal(schema, data, &value); err != nil {
		t.Fatal(err)
	}
	if value.Value != 1 || value.Other != 7 {
		t.Fatalf("decoded fields = %#v", value)
	}
}

type recordDeepShared struct{ recordValue }
type recordDeepLeft struct{ recordDeepShared }
type recordDeepRight struct{ recordDeepShared }
type recordDeepDiamond struct {
	recordDeepLeft
	recordDeepRight
}
type recordPointerDiamond struct {
	*recordLeft
	*recordRight
}

func TestRecordAmbiguousVariants(t *testing.T) {
	required := MustParse(`{"type":"record","name":"Required","fields":[{"name":"value","type":"int"}]}`)
	defaulted := MustParse(`{"type":"record","name":"Defaulted","fields":[{"name":"value","type":"int","default":5}]}`)
	for _, target := range []any{
		&recordDeepDiamond{}, &recordPointerDiamond{},
		&struct {
			First  int32 `avro:"value" json:"first" yaml:"first" xml:"first"`
			Second int32 `avro:"value" json:"second" yaml:"second" xml:"second"`
		}{First: 1, Second: 2},
	} {
		api := Config{}.Freeze()
		before := reflect.ValueOf(target).Elem().Interface()
		if _, err := api.Marshal(required, target); err == nil {
			t.Errorf("encoded ambiguous %T", target)
		}
		data, err := api.Marshal(defaulted, target)
		if err != nil || !bytes.Equal(data, []byte{10}) {
			t.Fatalf("ambiguous default %T = %x, %v", target, data, err)
		}
		if err := api.Unmarshal(required, []byte{14}, target); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, reflect.ValueOf(target).Elem().Interface()) {
			t.Errorf("ambiguous destination changed: %T", target)
		}
	}
}

type recordTaggedValue struct {
	Other int32 `avro:"Value" json:"other" yaml:"other" xml:"other"`
}

func TestRecordShallowFieldDominance(t *testing.T) {
	api := Config{}.Freeze()
	lower := MustParse(`{"type":"record","name":"Shallow","fields":[{"name":"value","type":"int"}]}`)
	value := struct {
		recordLeft
		Value int32 `avro:"value" json:"value" yaml:"value" xml:"value"`
	}{recordLeft: recordLeft{recordValue{Value: 1}}, Value: 3}
	data, err := api.Marshal(lower, value)
	if err != nil || !bytes.Equal(data, []byte{6}) {
		t.Fatalf("shallow field = %x, %v", data, err)
	}
	if err := api.Unmarshal(lower, []byte{14}, &value); err != nil {
		t.Fatal(err)
	}
	if value.Value != 7 || value.recordLeft.Value != 1 {
		t.Fatal("decoding selected the deeper field")
	}

	upper := MustParse(`{"type":"record","name":"ShallowUntagged","fields":[{"name":"Value","type":"int"}]}`)
	untagged := struct {
		recordTaggedValue
		Value int32 `json:"value" yaml:"value" xml:"value"`
	}{recordTaggedValue: recordTaggedValue{Other: 2}, Value: 1}
	data, err = api.Marshal(upper, untagged)
	if err != nil || !bytes.Equal(data, []byte{2}) {
		t.Fatalf("deeper tag hid shallow field: %x, %v", data, err)
	}
	if err := api.Unmarshal(upper, []byte{14}, &untagged); err != nil {
		t.Fatal(err)
	}
	if untagged.Value != 7 || untagged.Other != 2 {
		t.Fatal("deeper tag selected for decoding")
	}
}

type recordRecursive struct {
	*recordRecursive
	Value int32 `avro:"value" json:"value" yaml:"value" xml:"value"`
}
type recordRecursiveEmpty struct{ *recordRecursiveEmpty }

func TestRecordPointerAndRecursiveFields(t *testing.T) {
	api := Config{}.Freeze()
	schema := MustParse(`{"type":"record","name":"Pointer","fields":[{"name":"value","type":"int"}]}`)
	var value struct{ *recordValue }
	if err := api.Unmarshal(schema, []byte{14}, &value); err != nil {
		t.Fatal(err)
	}
	if value.recordValue == nil || value.Value != 7 {
		t.Fatal("decoder did not allocate the selected embedding")
	}
	data, err := api.Marshal(schema, value)
	if err != nil || !bytes.Equal(data, []byte{14}) {
		t.Fatalf("pointer encoding = %x, %v", data, err)
	}
	var recursive recordRecursive
	if err := api.Unmarshal(schema, []byte{14}, &recursive); err != nil {
		t.Fatal(err)
	}
	data, err = api.Marshal(schema, recursive)
	if err != nil || !bytes.Equal(data, []byte{14}) || recursive.recordRecursive != nil {
		t.Fatalf("recursive embedding = %x, %v", data, err)
	}
	if _, err := api.Marshal(schema, recordRecursiveEmpty{}); err == nil {
		t.Fatal("recursive empty record acquired a field")
	}
}

func TestRecordEmbeddingTags(t *testing.T) {
	api := Config{}.Freeze()
	nested := MustParse(`{"type":"record","name":"Outer","fields":[{"name":"child","type":{"type":"record","name":"Inner","fields":[{"name":"value","type":"int"}]}}]}`)
	var named struct {
		*recordValue `avro:"child" json:"child" yaml:"child" xml:"child"`
	}
	if err := api.Unmarshal(nested, []byte{14}, &named); err != nil {
		t.Fatal(err)
	}
	data, err := api.Marshal(nested, named)
	if err != nil || !bytes.Equal(data, []byte{14}) || named.Value != 7 {
		t.Fatalf("named embedding = %x, %v", data, err)
	}
	flat := MustParse(`{"type":"record","name":"Flat","fields":[{"name":"value","type":"int"}]}`)
	ignored := struct {
		recordValue `avro:"-"`
	}{recordValue{Value: 1}}
	if _, err := api.Marshal(flat, ignored); err == nil {
		t.Fatal("ignored embedding was promoted")
	}
	if err := api.Unmarshal(flat, []byte{14}, &ignored); err != nil || ignored.Value != 1 {
		t.Fatal("ignored embedding was decoded")
	}
	upper := MustParse(`{"type":"record","name":"EmptyTag","fields":[{"name":"Value","type":"int"}]}`)
	empty := struct {
		Value int32 `avro:",unused" json:"value" yaml:"value" xml:"value"`
	}{Value: 2}
	data, err = api.Marshal(upper, empty)
	if err != nil || !bytes.Equal(data, []byte{4}) {
		t.Fatalf("empty tag lost the Go field name: %x, %v", data, err)
	}
	if err := api.Unmarshal(upper, []byte{14}, &empty); err != nil || empty.Value != 7 {
		t.Fatal("empty tag was not decoded")
	}
}
