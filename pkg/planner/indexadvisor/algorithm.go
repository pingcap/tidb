// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexadvisor

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	s "github.com/pingcap/tidb/pkg/util/set"
)

/*
	This algorithm resembles the index selection algorithm published in 1997 by Chaudhuri
	and Narasayya. Details can be found in the original paper:
	Surajit Chaudhuri, Vivek R. Narasayya: An Efficient Cost-Driven Index Selection
	Tool for Microsoft Query Server. VLDB 1997: 146-155
	This implementation is the Golang version of
	https://github.com/hyrise/index_selection_evaluation/blob/refactoring/selection/algorithms/auto_admin_algorithm.py.
*/

// adviseIndexes implements the auto-admin algorithm.
func adviseIndexes(querySet s.Set[Query], indexableColSet s.Set[Column],
	optimizer Optimizer, option *Option) (s.Set[Index], error) {
	aa := &autoAdmin{
		optimizer: optimizer,
		option:    option,
		startAt:   time.Now(),
	}

	bestIndexes, err := aa.calculateBestIndexes(querySet, indexableColSet)
	if err != nil {
		return nil, err
	}
	return bestIndexes, nil
}

type autoAdmin struct {
	optimizer Optimizer
	option    *Option
	startAt   time.Time
}

func (aa *autoAdmin) calculateBestIndexes(querySet s.Set[Query], indexableColSet s.Set[Column]) (s.Set[Index], error) {
	if aa.option.MaxNumIndexes == 0 {
		return nil, nil
	}

	potentialIndexes := s.NewSet[Index]() // each indexable column as a single-column index
	for _, col := range indexableColSet.ToList() {
		potentialIndexes.Add(NewIndex(col.SchemaName, col.TableName, aa.tempIndexName(col), col.ColumnName))
	}
	if err := aa.timeout(); err != nil {
		return nil, err
	}

	currentBestIndexes := s.NewSet[Index]()
	for currentMaxIndexWidth := 1; currentMaxIndexWidth <= aa.option.MaxIndexWidth; currentMaxIndexWidth++ {
		candidates, err := aa.selectIndexCandidates(querySet, potentialIndexes)
		if err != nil {
			return nil, err
		}

		//maxIndexes := aa.option.MaxNumIndexes * (aa.option.MaxIndexWidth - currentMaxIndexWidth + 1)
		maxIndexes := aa.option.MaxNumIndexes
		currentBestIndexes, err = aa.enumerateCombinations(querySet, candidates, maxIndexes)
		if err != nil {
			return nil, err
		}

		if currentMaxIndexWidth < aa.option.MaxIndexWidth {
			// Update potential indexes for the next iteration
			potentialIndexes = currentBestIndexes
			potentialIndexes.Add(aa.createMultiColumnIndexes(indexableColSet, currentBestIndexes).ToList()...)
		}
		if err := aa.timeout(); err != nil {
			return nil, err
		}
	}

	currentBestIndexes, err := aa.heuristicMergeIndexes(currentBestIndexes, querySet)
	if err != nil {
		return nil, err
	}
	currentBestIndexes, err = aa.heuristicCoveredIndexes(currentBestIndexes, querySet)
	if err != nil {
		return nil, err
	}

	currentBestIndexes, err = aa.filterIndexes(querySet, currentBestIndexes)
	if err != nil {
		return nil, err
	}

	currentBestIndexes, err = aa.cutDown(currentBestIndexes, querySet, aa.optimizer, aa.option.MaxNumIndexes)
	if err != nil {
		return nil, err
	}

	if err := aa.timeout(); err != nil {
		return nil, err
	}

	// try to add more indexes if the number of indexes is less than maxIndexes
	for limit := 0; limit < 3 && currentBestIndexes.Size() < aa.option.MaxNumIndexes; limit++ {
		potentialIndexes = s.DiffSet(potentialIndexes, currentBestIndexes)
		currentCost, err := evaluateIndexSetCost(querySet, aa.optimizer, currentBestIndexes)
		if err != nil {
			return nil, err
		}
		currentBestIndexes, _, err = aa.enumerateGreedy(querySet, currentBestIndexes,
			currentCost, potentialIndexes, aa.option.MaxNumIndexes)
		if err != nil {
			return nil, err
		}

		currentBestIndexes, err = aa.filterIndexes(querySet, currentBestIndexes)
		if err != nil {
			return nil, err
		}
		if err := aa.timeout(); err != nil {
			return nil, err
		}
	}

	return currentBestIndexes, nil
}

// cutDown removes indexes from candidateIndexes until the number of indexes is less than or equal to maxIndexes.
func (aa *autoAdmin) cutDown(candidateIndexes s.Set[Index],
	querySet s.Set[Query], op Optimizer, maxIndexes int) (s.Set[Index], error) {
	if candidateIndexes.Size() <= maxIndexes {
		return candidateIndexes, nil
	}

	// find the target index to remove, which is the one that has the least impact on the cost.
	var bestCost IndexSetCost
	var targetIndex Index
	for i, idx := range candidateIndexes.ToList() {
		candidateIndexes.Remove(idx)
		cost, err := evaluateIndexSetCost(querySet, op, candidateIndexes)
		if err != nil {
			return nil, err
		}
		candidateIndexes.Add(idx)

		if i == 0 || cost.Less(bestCost) {
			bestCost = cost
			targetIndex = idx
		}
	}

	candidateIndexes.Remove(targetIndex)
	return aa.cutDown(candidateIndexes, querySet, op, maxIndexes)
}

func (aa *autoAdmin) heuristicCoveredIndexes(
	candidateIndexes s.Set[Index], querySet s.Set[Query]) (s.Set[Index], error) {
	// build an index (b, a) for `select a from t where b=1` to convert IndexLookup to IndexScan
	currentCost, err := evaluateIndexSetCost(querySet, aa.optimizer, candidateIndexes)
	if err != nil {
		return nil, err
	}

	for _, q := range querySet.ToList() {
		// parse select columns
		selectCols, err := CollectSelectColumnsFromQuery(q)
		if err != nil {
			return nil, err
		}
		if selectCols == nil || selectCols.Size() == 0 || selectCols.Size() > aa.option.MaxIndexWidth {
			continue
		}
		schemaName, tableName := selectCols.ToList()[0].SchemaName, selectCols.ToList()[0].TableName

		// generate cover-index candidates
		coverIndexSet := s.NewSet[Index]()
		for _, idx := range candidateIndexes.ToList() {
			if idx.SchemaName != schemaName || idx.TableName != tableName {
				continue // not for the same table
			}
			if len(idx.Columns)+selectCols.Size() > aa.option.MaxIndexWidth {
				continue // exceed the max-index-width limitation
			}
			// try this cover-index: idx-cols + select-cols
			var newCols []Column
			for _, col := range selectCols.ToList() {
				duplicated := false
				for _, idxCol := range idx.Columns {
					if col.Key() == idxCol.Key() {
						duplicated = true
						break
					}
				}
				if !duplicated {
					newCols = append(newCols, col)
				}
			}
			var cols []Column
			cols = append(cols, idx.Columns...)
			cols = append(cols, newCols...)
			coverIndexSet.Add(Index{
				SchemaName: schemaName,
				TableName:  tableName,
				IndexName:  aa.tempIndexName(cols...),
				Columns:    cols,
			})
		}

		// select the best cover-index
		var bestCoverIndex Index
		var bestCoverIndexCost IndexSetCost
		for i, coverIndex := range coverIndexSet.ToList() {
			if candidateIndexes.Contains(coverIndex) {
				continue // the new generated cover-index is duplicated
			}
			candidateIndexes.Add(coverIndex)
			cost, err := evaluateIndexSetCost(querySet, aa.optimizer, candidateIndexes)
			if err != nil {
				return nil, err
			}
			candidateIndexes.Remove(coverIndex)

			if i == 0 || cost.Less(bestCoverIndexCost) {
				bestCoverIndexCost = cost
				bestCoverIndex = coverIndex
			}
		}

		// check whether this cover-index can bring any benefits
		if bestCoverIndexCost.Less(currentCost) {
			candidateIndexes.Add(bestCoverIndex)
			currentCost = bestCoverIndexCost
		}
	}

	return candidateIndexes, nil
}

func (aa *autoAdmin) heuristicMergeIndexes(
	candidateIndexes s.Set[Index], querySet s.Set[Query]) (s.Set[Index], error) {
	// try to build index s.Set {(c1), (c2)} for predicate like `where c1=1 or c2=2` so that index-merge can be applied.
	currentCost, err := evaluateIndexSetCost(querySet, aa.optimizer, candidateIndexes)
	if err != nil {
		return nil, err
	}

	for _, q := range querySet.ToList() {
		// get all DNF columns from the query
		dnfCols, err := CollectDNFColumnsFromQuery(q)
		if err != nil {
			return nil, err
		}
		if dnfCols == nil || dnfCols.Size() == 0 {
			continue
		}
		orderByCols, err := CollectOrderByColumnsFromQuery(q)
		if err != nil {
			return nil, err
		}

		// create indexes for these DNF columns
		newIndexes := s.NewSet[Index]()
		for _, col := range dnfCols.ToList() {
			idx := NewIndex(col.SchemaName, col.TableName, aa.tempIndexName(col), col.ColumnName)
			contained := false
			for _, existingIndex := range candidateIndexes.ToList() {
				if existingIndex.PrefixContain(idx) {
					contained = true
					continue
				}
			}
			if !contained {
				newIndexes.Add(idx)
			}

			// index with DNF column + order-by column
			if len(orderByCols) == 0 {
				continue
			}
			cols := []Column{col}
			cols = append(cols, orderByCols...)
			if len(cols) > aa.option.MaxIndexWidth {
				cols = cols[:aa.option.MaxIndexWidth]
			}
			idx = NewIndexWithColumns(aa.tempIndexName(cols...), cols...)
			contained = false
			for _, existingIndex := range candidateIndexes.ToList() {
				if existingIndex.PrefixContain(idx) {
					contained = true
					continue
				}
			}
			if !contained {
				newIndexes.Add(idx)
			}
		}
		if newIndexes.Size() == 0 {
			continue
		}

		// check whether these new indexes for IndexMerge can bring some benefits.
		newCandidateIndexes := s.UnionSet(candidateIndexes, newIndexes)
		newCost, err := evaluateIndexSetCost(querySet, aa.optimizer, newCandidateIndexes)
		if err != nil {
			return nil, err
		}
		if newCost.Less(currentCost) {
			currentCost = newCost
			candidateIndexes, err = aa.filterIndexes(querySet, newCandidateIndexes)
			if err != nil {
				return nil, err
			}
		}
	}

	return candidateIndexes, nil
}

func (aa *autoAdmin) createMultiColumnIndexes(indexableColsSet s.Set[Column], indexes s.Set[Index]) s.Set[Index] {
	multiColumnCandidates := s.NewSet[Index]()
	for _, index := range indexes.ToList() {
		columns, err := aa.optimizer.TableColumns(index.SchemaName, index.TableName)
		if err != nil {
			continue
		}
		tableColsSet := s.ListToSet[Column](columns...)
		indexColsSet := s.ListToSet[Column](index.Columns...)
		for _, column := range s.DiffSet(s.AndSet(tableColsSet, indexableColsSet), indexColsSet).ToList() {
			cols := append([]Column{}, index.Columns...)
			cols = append(cols, column)
			multiColumnCandidates.Add(Index{
				SchemaName: index.SchemaName,
				TableName:  index.TableName,
				IndexName:  aa.tempIndexName(cols...),
				Columns:    cols,
			})
		}
	}
	return multiColumnCandidates
}

// filterIndexes filters some obviously unreasonable indexes.
// Rule 1: if index X is a prefix of index Y, then remove X.
// Rule 2: if index X has no any benefit, then remove X.
// Rule 3: if candidate index X is a prefix of some existing index in the workload, then remove X.
// Rule 4(TBD): remove unnecessary suffix columns, e.g. X(a, b, c) to X(a, b)
//
//	if no query can gain benefit from the suffix column c.
func (aa *autoAdmin) filterIndexes(querySet s.Set[Query], indexSet s.Set[Index]) (s.Set[Index], error) {
	indexList := indexSet.ToList()
	filteredIndexes := s.NewSet[Index]()
	originalCost, err := evaluateIndexSetCost(querySet, aa.optimizer, indexSet)
	if err != nil {
		return nil, err
	}
	for i, x := range indexList {
		filtered := false
		// rule 1
		for j, y := range indexList {
			if i == j {
				continue
			}
			if y.PrefixContain(x) {
				filtered = true
				continue
			}
		}
		if filtered {
			continue
		}

		// rule 2
		indexSet.Remove(x)
		newCost, err := evaluateIndexSetCost(querySet, aa.optimizer, indexSet)
		if err != nil {
			return nil, err
		}
		indexSet.Add(x)
		if !originalCost.Less(newCost) {
			continue
		}

		// rule 3
		prefixContain, err := aa.optimizer.PrefixContainIndex(x)
		if err != nil {
			return nil, err
		}
		if prefixContain {
			continue
		}

		filteredIndexes.Add(x)
	}
	return filteredIndexes, nil
}

// selectIndexCandidates selects the best indexes for each single-query.
func (aa *autoAdmin) selectIndexCandidates(querySet s.Set[Query], potentialIndexes s.Set[Index]) (s.Set[Index], error) {
	candidates := s.NewSet[Index]()
	for _, query := range querySet.ToList() {
		tmpQuerySet := s.ListToSet(query) // each query as a workload
		indexes, err := aa.potentialIndexesForQuery(query, potentialIndexes)
		if err != nil {
			return nil, err
		}

		bestPerQuery := 3 // keep 3 best indexes for each single-query
		bestQueryIndexes := s.NewSet[Index]()
		for i := 0; i < bestPerQuery; i++ {
			best, err := aa.enumerateCombinations(tmpQuerySet, indexes, 1)
			if err != nil {
				return nil, err
			}
			if best.Size() == 0 {
				break
			}
			bestQueryIndexes.Add(best.ToList()...)
			for _, index := range best.ToList() {
				indexes.Remove(index)
			}
			if bestQueryIndexes.Size() > bestPerQuery {
				break
			}
		}

		candidates.Add(bestQueryIndexes.ToList()...)
	}
	return candidates, nil
}

// potentialIndexesForQuery returns best recommended indexes of this workload from these candidates.
func (aa *autoAdmin) enumerateCombinations(
	querySet s.Set[Query],
	candidateIndexes s.Set[Index],
	maxNumberIndexes int) (s.Set[Index], error) {
	maxIndexesNative := 2
	if candidateIndexes.Size() > 50 {
		maxIndexesNative = 1
	}
	numberIndexesNaive := min(maxIndexesNative, candidateIndexes.Size(), maxNumberIndexes)
	currentIndexes, cost, err := aa.enumerateNaive(querySet, candidateIndexes, numberIndexesNaive)
	if err != nil {
		return nil, err
	}

	numberIndexes := min(maxNumberIndexes, candidateIndexes.Size())
	indexes, _, err := aa.enumerateGreedy(querySet, currentIndexes, cost, candidateIndexes, numberIndexes)
	return indexes, err
}

// enumerateGreedy finds the best combination of indexes with a greedy algorithm.
func (aa *autoAdmin) enumerateGreedy(querySet s.Set[Query], currentIndexes s.Set[Index],
	currentCost IndexSetCost, candidateIndexes s.Set[Index], numberIndexes int) (s.Set[Index], IndexSetCost, error) {
	if currentIndexes.Size() >= numberIndexes || candidateIndexes.Size() == 0 {
		return currentIndexes, currentCost, nil
	}

	// iterate all unused indexes and add one into the current s.Set
	indexCombinations := make([]s.Set[Index], 0, 128)
	for _, index := range candidateIndexes.ToList() {
		newCombination := s.UnionSet(currentIndexes, s.ListToSet(index))
		if newCombination.Size() != currentIndexes.Size()+1 {
			continue // duplicated index
		}
		indexCombinations = append(indexCombinations, newCombination)
	}
	if len(indexCombinations) == 0 {
		return currentIndexes, currentCost, nil
	}

	// find the best s.Set
	bestSet, bestCost, err := chooseBestIndexSet(querySet, aa.optimizer, indexCombinations)
	if err != nil {
		return nil, bestCost, err
	}
	if bestSet.Size() == 0 {
		return currentIndexes, currentCost, nil
	}
	bestNewIndex := s.DiffSet(bestSet, currentIndexes).ToList()[0]
	if bestCost.Less(currentCost) {
		currentIndexes.Add(bestNewIndex)
		candidateIndexes.Remove(bestNewIndex)
		currentCost = bestCost
		return aa.enumerateGreedy(querySet, currentIndexes, currentCost, candidateIndexes, numberIndexes)
	}

	return currentIndexes, currentCost, nil
}

// enumerateNaive enumerates all possible combinations of indexes with
// at most numberIndexesNaive indexes and returns the best one.
func (aa *autoAdmin) enumerateNaive(querySet s.Set[Query],
	candidateIndexes s.Set[Index], numberIndexesNaive int) (s.Set[Index], IndexSetCost, error) {
	// get all index combinations
	indexCombinations := make([]s.Set[Index], 0, 128)
	for numberOfIndexes := 0; numberOfIndexes <= numberIndexesNaive; numberOfIndexes++ {
		indexCombinations = append(indexCombinations, s.CombSet(candidateIndexes, numberOfIndexes)...)
	}

	lowestCostIndexes, lowestCost, err := chooseBestIndexSet(querySet, aa.optimizer, indexCombinations)
	if err != nil {
		return nil, lowestCost, err
	}

	return lowestCostIndexes, lowestCost, nil
}

func (aa *autoAdmin) potentialIndexesForQuery(query Query, potentialIndexes s.Set[Index]) (s.Set[Index], error) {
	indexes := s.NewSet[Index]()
	for _, index := range potentialIndexes.ToList() {
		// The leading index column must be referenced by the query.
		indexableColSetOfQuery, err := CollectIndexableColumnsFromQuery(query, aa.optimizer)
		if err != nil {
			return nil, err
		}
		if indexableColSetOfQuery.Contains(index.Columns[0]) {
			indexes.Add(index)
		}
	}
	return indexes, nil
}

// tempIndexName returns a temp index name for the given columns.
func (*autoAdmin) tempIndexName(cols ...Column) string {
	names := make([]string, 0, len(cols))
	for _, col := range cols {
		names = append(names, col.ColumnName)
	}
	idxName := fmt.Sprintf("idx_%v", strings.Join(names, "_"))
	if len(idxName) <= 64 {
		return idxName
	}

	return fmt.Sprintf("idx_%v", uuid.New().String())
}

func (aa *autoAdmin) timeout() error {
	if time.Since(aa.startAt) > aa.option.Timeout {
		return fmt.Errorf("index advisor timeout after %v", aa.option.Timeout)
	}
	return nil
}
