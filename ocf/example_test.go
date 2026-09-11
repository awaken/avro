package ocf_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/awaken/avro/v2/ocf"
)

func TestExampleNewEncoderUsesWritableFile(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "example_test.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	var body *ast.BlockStmt
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == "ExampleNewEncoder" {
			body = fn.Body
			break
		}
	}
	if body == nil {
		t.Fatal("ExampleNewEncoder not found")
	}

	var openFile func(string) (*os.File, error)
	var openName string
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if ok && pkg.Name == "os" && (sel.Sel.Name == "Create" || sel.Sel.Name == "Open") {
			openName = sel.Sel.Name
			if openName == "Create" {
				openFile = os.Create
			} else {
				openFile = os.Open
			}
		}
		return true
	})
	if openFile == nil {
		t.Fatal("ExampleNewEncoder file operation not found")
	}

	dir, err := os.MkdirTemp(filepath.Join("..", "..", "..", "..", "tmp"), "avro-ocf-example-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	path := filepath.Join(dir, "file.avro")
	seed, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = seed.Close(); err != nil {
		t.Fatal(err)
	}

	output, err := openFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()

	encoder, err := ocf.NewEncoder(`"null"`, output)
	if err != nil {
		t.Fatalf("ExampleNewEncoder uses os.%s: %v", openName, err)
	}
	if err = encoder.Close(); err != nil {
		t.Fatal(err)
	}
}

func ExampleNewDecoder() {
	type SimpleRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}

	f, err := os.Open("/your/avro/file.avro")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	dec, err := ocf.NewDecoder(f)
	if err != nil {
		log.Fatal(err)
	}

	for dec.HasNext() {
		var record SimpleRecord
		err = dec.Decode(&record)
		if err != nil {
			log.Fatal(err)
		}

		// Do something with the data
	}

	if err := dec.Error(); err != nil {
		log.Fatal(err)
	}
}

func ExampleNewEncoder() {
	schema := `{
	    "type": "record",
	    "name": "simple",
	    "namespace": "org.hamba.avro",
	    "fields" : [
	        {"name": "a", "type": "long"},
	        {"name": "b", "type": "string"}
	    ]
	}`

	type SimpleRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}

	f, err := os.Create("/your/avro/file.avro")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	enc, err := ocf.NewEncoder(schema, f)
	if err != nil {
		log.Fatal(err)
	}

	var record SimpleRecord
	err = enc.Encode(record)
	if err != nil {
		log.Fatal(err)
	}

	if err := enc.Flush(); err != nil {
		log.Fatal(err)
	}

	if err := f.Sync(); err != nil {
		log.Fatal(err)
	}
}
