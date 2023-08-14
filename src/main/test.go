package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// import "6.5840/mr"
// import "plugin"

// import "encoding/json"

var mu sync.Mutex

func main() {
	mu.Lock()
	defer mu.Unlock()
	go func() {
		mu.Lock()
		defer mu.Unlock()
		fmt.Println("test")
	}()
	time.Sleep(time.Second)
}


func testTempfile() {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "example-*")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Temp file created:", tempFile.Name())
	time.Sleep(time.Second)
	defer func() {
		err := tempFile.Close()
		if err != nil {
			log.Fatal(err)
		}

		err = os.Remove(tempFile.Name())
		if err != nil {
			log.Fatal(err)
		}
	}()
}

func testJson() {
	
}

func testItoa() {
	str := []string{"1", "1", "1"}
	num := strconv.Itoa(len(str))
	fmt.Printf("%T", num)
}

type Person struct {
	Name string
	Age  int
}
type ByAge []Person
func (a ByAge) Len() int           { return len(a) }
func (a ByAge) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByAge) Less(i, j int) bool { return a[i].Age < a[j].Age }

func testSort() {
	
	people := []Person{
		{Name: "Alice", Age: 25},
		{Name: "Bob", Age: 20},
		{Name: "Charlie", Age: 30},
	}
	sort.Sort(ByAge(people))
	fmt.Println(people)
	fmt.Printf("%T", people)
}

func testStrings() {
	content := "Hi, Im Jehan Rio"
	ff := func(r rune) bool { return !unicode.IsLetter(r)}
	fields := strings.FieldsFunc(content, ff)
	for _, c := range(fields) {
		fmt.Println(c)
	}
}

func testIO() {
	filename := "mytext.txt"
	file, _ := os.Open(filename)
	content, _ := ioutil.ReadAll(file)
	file.Close()
	fmt.Println(string(content))
}

func testFile() {
	name := "jehanrio"
	if name != "" {
		log.Fatalf("name is: %s", name)
	}
}

// func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
// 	p, err := plugin.Open(filename)
// 	if err != nil {
// 		log.Fatalf("cannot load plugin %v", filename)
// 	}
// 	xmapf, err := p.Lookup("Map")
// 	if err != nil {
// 		log.Fatalf("cannot find Map in %v", filename)
// 	}
// 	fmt.Printf("%T\n", xmapf)
// 	mapf := xmapf.(func(string, string) []mr.KeyValue)
// 	fmt.Printf("%T\n", mapf)
// 	xreducef, err := p.Lookup("Reduce")
// 	if err != nil {
// 		log.Fatalf("cannot find Reduce in %v", filename)
// 	}
// 	fmt.Printf("%T\n", xreducef)
// 	reducef := xreducef.(func(string, []string) string)


// 	return mapf, reducef
// }
