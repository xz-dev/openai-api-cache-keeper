package main

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"runtime"
	"strings"
	"testing"
)

// validJSONStream must accept exactly what json.Valid accepts.
func TestValidJSONStreamMatchesStdlib(t *testing.T) {
	cases := []string{
		``, ` `, `0`, `-0`, `01`, `-`, `1.`, `1.5`, `1e`, `1e+`, `1E-7`, `.5`, `1.5e3x`, `true`, `tru`, `nul`, `null `,
		`"a"`, `"\u12"`, `"\u12g4"`, `"\uABCD"`, `"\x"`, `"\/"`, "\"\x01\"", "\"\xff\"", `"`, `"a`, `{}`, `[]`, `{`, `[`,
		`{"a":1}`, `{"a":1,}`, `{"a"}`, `{1:2}`, `[1,]`, `[,1]`, `[1 2]`, `[1,[2,{"x":[]}]]`, `{"a":{"b":{"c":[1,2,3]}}}`,
		` {"a" : [ true , false , null ] } `, `{} {}`, `[]]`, `{"a":1}}`, "\t[\r\n]\n", `[1]x`, `{"a":"b"`, `{"a":`,
		strings.Repeat("[", 10000) + strings.Repeat("]", 10000), strings.Repeat("[", 10001) + strings.Repeat("]", 10001),
	}
	r := rand.New(rand.NewSource(1))
	alphabet := []byte(`{}[]:,"\\ \t\n0123456789-+.eEtrufalsn/ux` + "\x01\xff")
	for i := 0; i < 20000; i++ {
		b := make([]byte, r.Intn(24))
		for j := range b {
			b[j] = alphabet[r.Intn(len(alphabet))]
		}
		cases = append(cases, string(b))
	}
	// Mutations of valid documents exercise deeper grammar paths.
	seed := []byte(`{"model":"m","input":[{"role":"user","content":"x\"y\u00e9"},1.5e-3,-0,true,null,[]],"n":{}}`)
	for i := 0; i < 20000; i++ {
		b := bytes.Clone(seed)
		for k := r.Intn(3); k >= 0; k-- {
			b[r.Intn(len(b))] = alphabet[r.Intn(len(alphabet))]
		}
		cases = append(cases, string(b))
	}
	for _, c := range cases {
		want := json.Valid([]byte(c))
		got := validJSONStream(context.Background(), strings.NewReader(c)) == nil
		if got != want {
			t.Fatalf("validJSONStream(%q)=%v, json.Valid=%v", c[:min(len(c), 120)], got, want)
		}
	}
}

func TestValidJSONStreamConstantMemory(t *testing.T) {
	// A 64 MiB string validates through the fixed 32 KiB buffer.
	body := `{"input":"` + strings.Repeat("x", 64<<20) + `"}`
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	if err := validJSONStream(context.Background(), strings.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&b)
	if d := b.TotalAlloc - a.TotalAlloc; d > uint64(jsonValidateCredit) {
		t.Fatalf("validation allocated %d bytes for a %d-byte body; credit is %d", d, len(body), jsonValidateCredit)
	}
}
