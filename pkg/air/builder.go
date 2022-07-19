// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package air

import (
	"fmt"
	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/array"
	"github.com/apache/arrow/go/v9/arrow/memory"
	"otel-arrow-adapter/pkg/air/column"
	config2 "otel-arrow-adapter/pkg/air/config"
	"otel-arrow-adapter/pkg/air/dictionary"
	"otel-arrow-adapter/pkg/air/rfield"
	"otel-arrow-adapter/pkg/air/stats"
	"sort"
)

type OrderBy struct {
	FieldPaths [][]int
}

// A Record builder.
// Must be fed with homogeneous records.
type RecordBuilder struct {
	// The configuration of the builder.
	config *config2.Config

	// The dictionary id generator.
	dictIdGen dictionary.DictIdGenerator

	// The columns of the Record builder.
	columns column.Columns

	// The path for each fields.
	fieldPaths []*rfield.FieldPath

	// Optional order by clause
	orderBy *OrderBy

	// Non ordered records
	recordList []*Record

	// Flag to indicate if the builder has been optimized.
	optimized bool
}

type RecordBuilderMetadata struct {
	SchemaId        string
	Columns         []*column.ColumnMetadata
	RecordListLen   int
	Optimized       bool
	DictionaryStats []*stats.DictionaryStats
}

// Constructs a new `RecordBuilder` from a Record.
func NewRecordBuilderWithRecord(record *Record, config *config2.Config) *RecordBuilder {
	fieldPath := make([]*rfield.FieldPath, 0, record.FieldCount())
	builder := RecordBuilder{
		config:     config,
		dictIdGen:  dictionary.DictIdGenerator{Id: 0},
		columns:    column.Columns{},
		fieldPaths: fieldPath,
		orderBy:    nil,
		recordList: nil,
		optimized:  config.Dictionaries.StringColumns.MaxSortedDictionaries == 0,
	}

	for fieldIdx := range record.fields {
		fieldPath := builder.columns.CreateColumn([]int{fieldIdx}, record.fields[fieldIdx], config, &builder.dictIdGen)
		if fieldPath != nil {
			builder.fieldPaths = append(builder.fieldPaths, fieldPath)
		}
	}
	return &builder
}

func (rbb *RecordBuilder) AddRecord(record *Record) {
	if rbb.recordList != nil {
		rbb.recordList = append(rbb.recordList, record)
	} else {
		for fieldIdx := range record.fields {
			rbb.columns.UpdateColumn(rbb.fieldPaths[fieldIdx], record.fields[fieldIdx])
		}
	}
}

func (rbb *RecordBuilder) IsEmpty() bool {
	return rbb.columns.IsEmpty()
}

func (rbb *RecordBuilder) Build(allocator *memory.GoAllocator) (arrow.Record, error) {
	// Sorts the string columns according to the order by clause.
	if rbb.orderBy != nil {
		recordList := rbb.recordList
		capacity := 100
		if len(recordList) > capacity {
			capacity = len(recordList)
		}
		rbb.recordList = make([]*Record, 0, capacity)
		sortByRecordList(recordList, rbb.orderBy)
		for _, record := range recordList {
			for pos := range record.fields {
				rbb.columns.UpdateColumn(rbb.fieldPaths[pos], record.fields[pos])
			}
		}
	}

	// Creates a column builder for every column.
	fields, builders, err := rbb.columns.Build(allocator)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, nil
	}

	// Creates an Arrow Schema from the fields returned by the build method.
	schema := arrow.NewSchema(fields, nil)
	cols := make([]arrow.Array, len(fields))
	rows := int64(0)

	defer func(cols []arrow.Array) {
		for _, col := range cols {
			if col == nil {
				continue
			}
			col.Release()
		}
	}(cols)

	// Creates the Record from the schema and columns.
	for i, builder := range builders {
		cols[i] = builder.NewArray()
		irow := int64(cols[i].Len())
		if i > 0 && irow != rows {
			panic(fmt.Errorf("arrow/array: field %d has %d rows. want=%d", i, irow, rows))
		}
		rows = irow
		builder.Release()
	}

	return array.NewRecord(schema, cols, rows), nil
}

func (rbb *RecordBuilder) Metadata(schemaId string) *RecordBuilderMetadata {
	recordListLen := 0

	if rbb.recordList != nil {
		recordListLen = len(rbb.recordList)
	}

	return &RecordBuilderMetadata{
		SchemaId:        schemaId,
		Columns:         rbb.columns.Metadata(),
		RecordListLen:   recordListLen,
		Optimized:       rbb.optimized,
		DictionaryStats: rbb.columns.DictionaryStats(),
	}
}

func (rbb *RecordBuilder) DictionaryStats() []*stats.DictionaryStats {
	return rbb.columns.DictionaryStats()
}

func (rbb *RecordBuilder) OrderBy(fieldPaths [][]int) {
	rbb.orderBy = &OrderBy{
		FieldPaths: fieldPaths,
	}
	rbb.recordList = []*Record{}
}

func (rbb *RecordBuilder) Optimize() bool {
	if rbb.optimized {
		return true
	}

	if rbb.orderBy == nil {
		var dictionaryStats []*stats.DictionaryStats
		for _, ds := range rbb.DictionaryStats() {
			if ds.Cardinality > 1 && rbb.config.Dictionaries.StringColumns.IsDictionary(ds.TotalEntry, ds.Cardinality) {
				dictionaryStats = append(dictionaryStats, ds)
			}
		}
		sort.Sort(stats.DictionaryStatsSlice(dictionaryStats))
		var paths [][]int
		for i, ds := range dictionaryStats {
			if i < rbb.config.Dictionaries.StringColumns.MaxSortedDictionaries {
				path := make([]int, len(ds.Path))
				copy(path, ds.Path)
				paths = append(paths, path)
			} else {
				break
			}
		}
		if len(paths) > 0 {
			rbb.orderBy = &OrderBy{
				FieldPaths: paths,
			}
			rbb.optimized = true
			rbb.recordList = []*Record{}
			return true
		}
	}
	return false
}

func sortByRecordList(recordList []*Record, orderBy *OrderBy) {
	if orderBy == nil {
		return
	}

	records := Records{
		records: recordList,
		orderBy: orderBy,
	}
	sort.Sort(&records)
}