package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/iancoleman/strcase"
	"github.com/jinzhu/copier"
	"github.com/jinzhu/inflection"
	"github.com/kenshaw/snaker"
)

var (
	schemaFile = flag.String("schema", "", "input schema file")
	out        = flag.String("o", "", "output file")
)

type TypeBase = string

var baseTypes = map[string]string{
	"int":       "int64",
	"integer":   "int64",
	"smallint":  "int64",
	"mediumint": "int64",
	"bigint":    "int64",
	"tinyint":   "int64",
	"bool":      "bool",
	"float":     "float64",
	"double":    "float64",
	"decimal":   "string",
	"numeric":   "string",
	"char":      "string",
	"varchar":   "string",
	"text":      "string",
	"blob":      "string",
	"json":      "string",
	"binary":    "[]byte",
	"varbinary": "[]byte",
	"timestamp": "int64",
	"date":      "civil.Date",
	"datetime":  "time.Time",
}

type TypeLen = int64
type Type struct {
	Array bool     `json:"array"`
	Base  TypeBase `json:"-"`
	Len   TypeLen  `json:"len"`
}
type ColumnDef struct {
	Name           string `json:"namesDb"`
	NameDbSingular string `json:"nameDb"`
	NameJson       string `json:"nameJson"`
	GoName         string `json:"Name"`
	GoVarName      string `json:"name"`
	GoNames        string `json:"Names"`
	GoVarNames     string `json:"names"`
	NameExactJson  string `json:"nameExact"`
	GoType         string `json:"Type"`
	GoBaseType     string `json:"baseType"`
	Size           int64  `json:"size,omitempty"`
	Type           Type   `json:"-"`
	IsArray        bool   `json:"isArray"`
	NotNull        bool   `json:"notNull"`
	AutoIncrement  bool   `json:"autoIncrement,omitempty"`
	Default        string `json:"default,omitempty"`
	Comment        string `json:"comment,omitempty"`
	Key            string `json:"key"`
}

func lowerCamel(s string) string {
	if s == "" {
		return ""
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToLower(r)) + s[n:]
}

var shortNameRe = regexp.MustCompile("[A-Z]")

func shortName(s string) string {
	return strings.ToLower(strings.Join(shortNameRe.FindAllString(s, -1), ""))
}
func (x ColumnDef) MarshalJSON() ([]byte, error) {
	x.NameExactJson = x.Name
	x.NameDbSingular = inflection.Singular(x.Name)
	x.GoName = snaker.SnakeToCamel(x.NameDbSingular)
	x.GoVarName = lowerCamel(x.GoName)
	x.NameJson = strcase.ToLowerCamel(x.Name)
	x.GoNames = snaker.SnakeToCamel(plural(x.Name))
	if strings.HasSuffix(x.GoNames, "ids") {
		x.GoNames = x.GoNames[:len(x.GoNames)-3] + "Ids"
	}
	x.GoVarNames = strcase.ToLowerCamel(x.GoNames)
	if x.NameJson != "id" && strings.HasSuffix(x.NameJson, "id") {
		x.NameJson = x.NameJson[:len(x.NameJson)-2] + "Id"
	}
	x.Key = x.NameJson
	x.GoType = baseTypes[x.Type.Base]
	x.Size = x.Type.Len
	x.IsArray = x.Type.Array
	if !x.NotNull {
		x.GoType = "*" + x.GoType
	}
	x.GoBaseType = x.GoType
	if x.Type.Array {
		x.GoType = "[]" + x.GoType
	}
	type MyColumnDef ColumnDef
	return json.Marshal(MyColumnDef(x))
}

type OnDelete int

const (
	NoActionOnDelete OnDelete = iota
	CascadeOnDelete
)

type Interleave struct {
	Parent   string   `json:"string"`
	OnDelete OnDelete `json:"onDelete"`
}
type KeyPart struct {
	Column         string `json:"namesDb"`
	NameDbSingular string `json:"nameDb"`
	GoName         string `json:"Name"`
	GoVarName      string `json:"name"`
	GoNames        string `json:"Names"`
	GoVarNames     string `json:"names"`
	GoType         string `json:"Type"`
	GoBaseType     string `json:"baseType"`
}

func (x KeyPart) MarshalJSON() ([]byte, error) {
	x.NameDbSingular = inflection.Singular(x.Column)
	x.GoName = snaker.SnakeToCamel(x.NameDbSingular)
	x.GoVarName = lowerCamel(x.GoName)
	x.GoNames = snaker.SnakeToCamel(plural(x.Column))
	x.GoVarNames = lowerCamel(x.GoNames)
	type MyKeyPart KeyPart
	return json.Marshal(MyKeyPart(x))
}

type TableConstraint struct {
	Name       string
	ForeignKey ForeignKey
}
type ForeignKey struct {
	Columns    []*KeyPart
	RefTable   string
	RefColumns []string
}
type Table struct {
	Name            string              `json:"namesDb"`
	NameDbSingular  string              `json:"nameDb"`
	GoName          string              `json:"Name"`
	GoVarName       string              `json:"name"`
	GoNames         string              `json:"Names"`
	GoVarNames      string              `json:"names"`
	GoShortName     string              `json:"n"`
	Key             string              `json:"key"`
	Columns         []*ColumnDef        `json:"fields"`
	PrimaryKey      []*KeyPart          `json:"primaryKey"`
	Interleave      *Interleave         `json:"interleave,omitempty"`
	Indexes         []*CreateIndex      `json:"indexes,omitempty"`
	Constraints     []*TableConstraint  `json:"-"`
	Children        []string            `json:"children,omitempty"`
	RefTables       []string            `json:"refTables,omitempty"`
	Descendents     map[string]struct{} `json:"-"`
	DependencyOrder int                 `json:"dependencyOrder"`
}

func (x Table) MarshalJSON() ([]byte, error) {
	x.GoName = snaker.SnakeToCamel(x.NameDbSingular)
	x.GoVarName = lowerCamel(x.GoName)
	x.GoNames = snaker.SnakeToCamel(plural(x.Name))
	x.GoVarNames = lowerCamel(x.GoNames)
	x.GoShortName = shortName(x.GoName)
	type MyTable Table
	return json.Marshal(MyTable(x))
}

type CreateIndex struct {
	Name         string     `json:"name"`
	Table        string     `json:"table"`
	Columns      []*KeyPart `json:"fields"`
	Unique       bool       `json:"unique"`
	NullFiltered bool       `json:"nullFiltered,omitempty"`
	Storing      []string   `json:"storing,omitempty"`
	Interleave   string     `json:"interleave,omitempty"`
}

func parseDDL(in []byte) ([]*Table, error) {
	ddl, err := convert(in)
	if err != nil {
		return nil, err
	}
	var colMap map[string]map[string]*ColumnDef
	tblMap := make(map[string]*Table, len(ddl))
	tables := make([]*Table, 0, len(ddl))
	colMap = make(map[string]map[string]*ColumnDef)
	for _, l := range ddl {
		switch v := l.(type) {
		case *sqlparser.CreateTable:
			tbl := &Table{Indexes: []*CreateIndex{}}
			if err := copier.Copy(tbl, v); err != nil {
				return nil, err
			}
			name := v.DDL.NewName.Name.String()
			tbl.Name = name
			tbl.NameDbSingular = inflection.Singular(tbl.Name)
			tbl.Key = snaker.ForceCamelIdentifier(tbl.NameDbSingular)
			colMap[name] = make(map[string]*ColumnDef)
			tbl.Columns = make([]*ColumnDef, len(v.Columns))
			for i, c := range v.Columns {
				var notNull, autoIncrement bool
				var defaultVal, comment string
				for _, x := range c.Options {
					if x.Type == 2 {
						notNull = true
					} else if x.Type == 3 {
						autoIncrement = true
					} else if x.Type == 4 {
						defaultVal = x.Value
					} else if x.Type == 9 {
						unq, err := strconv.Unquote(x.Value)
						if err != nil {
							return nil, err
						}
						comment = unq
					}
				}
				typeSplitted := strings.SplitN(c.Type, "(", 2)
				var typeLen int64
				if len(typeSplitted) > 1 {
					conv, err := strconv.ParseUint(strings.SplitN(typeSplitted[1], ")", 2)[0], 10, 64)
					if err != nil {
						return nil, err
					}
					typeLen = int64(conv)
				}
				base := typeSplitted[0]
				if typeLen == 1 {
					base = "bool"
				}
				colDef := &ColumnDef{
					Name: c.Name,
					Type: Type{
						Base: base,
						Len:  typeLen,
					},
					NotNull:       notNull,
					Default:       defaultVal,
					AutoIncrement: autoIncrement,
					Comment:       comment,
				}
				colDef.GoType = baseTypes[colDef.Type.Base]
				colDef.IsArray = colDef.Type.Array
				if !colDef.NotNull {
					colDef.GoType = "*" + colDef.GoType
				}
				colDef.GoBaseType = colDef.GoType
				if colDef.Type.Array {
					colDef.GoType = "[]" + colDef.GoType
				}
				tbl.Columns[i] = colDef
				colMap[name][c.Name] = colDef
			}
			for _, c := range v.Constraints {
				if c.Type == 1 {
					tbl.PrimaryKey = make([]*KeyPart, len(c.Keys))
					for i, x := range c.Keys {
						key := &KeyPart{
							Column: x.String(),
						}
						tbl.PrimaryKey[i] = key
					}
				} else if c.Type == 3 || c.Type == 4 {
					cols := make([]*KeyPart, len(c.Keys))
					for i, x := range c.Keys {
						key := &KeyPart{
							Column: x.String(),
						}
						cols[i] = key
					}
					idx := &CreateIndex{
						Name:    c.Name,
						Table:   tbl.Name,
						Columns: cols,
						Unique:  c.Type == 4,
					}
					tbl.Indexes = append(tbl.Indexes, idx)
				} else if c.Type == 7 {
					cols := make([]*KeyPart, len(c.Keys))
					for i, x := range c.Keys {
						key := &KeyPart{
							Column: x.String(),
						}
						cols[i] = key
					}
					cst := &TableConstraint{
						Name: c.Name,
						ForeignKey: ForeignKey{
							Columns: cols,
						},
					}
					tbl.Constraints = append(tbl.Constraints, cst)
				}
			}
			for _, p := range tbl.PrimaryKey {
				if c, ok := colMap[name][p.Column]; ok {
					p.GoType = baseTypes[c.Type.Base]
					p.GoBaseType = c.GoBaseType
				} else {
					log.Println("not found", p.Column)
				}
			}
			for _, index := range tbl.Indexes {
				for _, p := range index.Columns {
					if c, ok := colMap[name][p.Column]; ok {
						p.GoType = baseTypes[c.Type.Base]
						p.GoBaseType = c.GoBaseType
					} else {
						log.Println("not found", p.Column)
					}
				}
			}
			tables = append(tables, tbl)
			tblMap[name] = tbl
		case *sqlparser.DDL:
		default:
			log.Printf("unknown ddl type: %v\n", reflect.TypeOf(l))
		}
	}
	return tables, nil
}

type FileContent struct {
	FileKind string   `json:"kind"`
	SrcKind  string   `json:"srcKind"`
	Data     []*Table `json:"data"`
}

func collectDescendents(keys map[string]struct{}, m map[string]*Table, out *Table) {
	for k, _ := range keys {
		if out != nil {
			for x, _ := range m[k].Descendents {
				out.Descendents[x] = struct{}{}
			}
		}
		collectDescendents(m[k].Descendents, m, m[k])
	}
}
func convert(in []byte) ([]sqlparser.Statement, error) {
	ddl := string(in)
	stmtList := make([]sqlparser.Statement, 0)
	for _, m := range ddlRe.FindAllStringIndex(ddl, -1) {
		stmt, err := sqlparser.Parse(ddl[m[0]:])
		if err != nil {
			panic(err)
		}
		stmtList = append(stmtList, stmt)
	}
	return stmtList, nil
}

var ddlRe = regexp.MustCompile(`CREATE|ALTER`)

func process() error {
	b, err := os.ReadFile(*schemaFile)
	if err != nil {
		return err
	}
	parsed, err := parseDDL(b)
	if err != nil {
		return err
	}
	nameMap := make(map[string]*Table, len(parsed))
	for _, v := range parsed {
		nameMap[v.Name] = v
	}
	type pair struct {
		a, b string
	}
	added := map[pair]struct{}{}
	for _, v := range parsed {
		if v.Interleave != nil {
			nameMap[v.Interleave.Parent].Children = append(nameMap[v.Interleave.Parent].Children, v.Key)
		}
		for _, vv := range v.Constraints {
			if _, ok := added[pair{vv.ForeignKey.RefTable, v.Key}]; !ok {
				if _, ok := nameMap[vv.ForeignKey.RefTable]; ok {
					nameMap[vv.ForeignKey.RefTable].RefTables = append(nameMap[vv.ForeignKey.RefTable].RefTables, v.Key)
					added[pair{vv.ForeignKey.RefTable, v.Key}] = struct{}{}
				}
			}
		}
	}
	for _, v := range parsed {
		sort.Strings(v.Children)
		sort.Strings(v.RefTables)
	}
	for _, x := range parsed {
		if x.Descendents == nil {
			x.Descendents = make(map[string]struct{})
		}
		for _, v := range x.Children {
			x.Descendents[v] = struct{}{}
		}
		for _, v := range x.RefTables {
			x.Descendents[v] = struct{}{}
		}
	}
	keys := make(map[string]struct{}, len(parsed))
	for _, x := range parsed {
		keys[x.Key] = struct{}{}
	}
	parsedMap := map[string]*Table{}
	for _, x := range parsed {
		parsedMap[x.Key] = x
	}
	collectDescendents(keys, parsedMap, nil)
	sort.SliceStable(parsed, func(i, j int) bool {
		_, ok1 := parsed[j].Descendents[parsed[i].Key]
		_, ok2 := parsed[i].Descendents[parsed[j].Key]
		if ok1 != ok2 {
			return ok1
		}
		if len(parsed[i].Descendents) != len(parsed[j].Descendents) {
			return len(parsed[i].Descendents) < len(parsed[j].Descendents)
		}
		return parsed[i].Key < parsed[j].Key
	})
	for i, x := range parsed {
		x.DependencyOrder = i + 1
	}
	fileContent := FileContent{
		FileKind: "sql",
		SrcKind:  "sql",
		Data:     parsed,
	}
	parsedJson, err := json.MarshalIndent(fileContent, "", "\t")
	if err != nil {
		return err
	}
	if *out == "-" {
		if _, err := os.Stdout.Write(parsedJson); err != nil {
			return err
		}
	} else {
		outFile := *out
		if outFile == "" {
			outFile = strings.Replace(*schemaFile, ".sql", ".json", 1)
		}
		if err := ioutil.WriteFile(outFile, parsedJson, 0644); err != nil {
			return err
		}
	}
	return nil
}
func plural(s string) string {
	out := inflection.Plural(s)
	if out == "information" {
		return "informations"
	} else if out == "Information" {
		return "Informations"
	}
	return out
}
func main() {
	flag.Parse()
	if err := process(); err != nil {
		log.Fatalln(err)
	}
}
