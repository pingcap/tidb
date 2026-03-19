// Copyright 2025 PingCAP, Inc.
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

package generator

import (
	"encoding/json"
	"fmt"

	"github.com/openai/openai-go"
	"github.com/pingcap/tidb/tests/llmtest/logger"
	"github.com/pingcap/tidb/tests/llmtest/testcase"
	"go.uber.org/zap"
)

type dmlPromptGenerator struct {
}

// Name implements PromptGenerator.Name
func (g *dmlPromptGenerator) Name() string {
	return "dml"
}

// Groups implements PromptGenerator.Groups
func (g *dmlPromptGenerator) Groups() []string {
	return []string{
		"insert", "update", "delete",
	}
}

// GeneratePrompt implements PromptGenerator.GeneratePrompt
func (g *dmlPromptGenerator) GeneratePrompt(group string, count int, existCases []*testcase.Case) []openai.ChatCompletionMessageParamUnion {
	messages := make([]openai.ChatCompletionMessageParamUnion, 0, 2)

	systemPrompt := `You are a professional QA engineer testing a new SQL database compatible with MySQL. You are tasked with testing the compatibility of the database with MySQL for a specific DML operation. You should write the queries to cover the corner cases of the operation. The common cases are not needed. You should try to use this operation with different valid argument types to test the implicit type conversion. You should try to use this operation with NULL to test the behavior of NULL. Please return a valid JSON object with the key "queries" and an array of strings as the value. Be careful with the escape characters. You should avoid using NOW(), RAND() or any other functions that return different results on each call. You should pack the related DDL in the same query. You should CREATE and DROP the table before and after using it. You should use a SELECT statement to verify the result after each DML. The SELECT statement should have stable order.

    IMPORTANT: Don't put anything else in the response.

    EXAMPLE INPUT:
    Return 3 random SQL queries using this operation: INSERT.
    
    EXAMPLE JSON OUTPUT:
    {"queries": ["CREATE TABLE t (id INT, name VARCHAR(25));INSERT INTO t VALUES (1, 'a');SELECT * FROM t ORDER BY id;DROP TABLE t;", "CREATE TABLE t (id INT, name VARCHAR(25));INSERT INTO t VALUES (NULL, 'b');SELECT * FROM t ORDER BY id;DROP TABLE t;", "CREATE TABLE t (id INT, name VARCHAR(25));INSERT INTO t VALUES (1, '\\n');SELECT * FROM t ORDER BY id;DROP TABLE t;"]}`
	messages = append(messages, openai.SystemMessage(systemPrompt))

	userPromptTemplate := `Return %d random SQL queries using this operation: %s.`

	if len(existCases) > 0 {
		messages = append(messages, openai.UserMessage(fmt.Sprintf(userPromptTemplate, len(existCases), group)))

		existResponse := make([]string, 0, len(existCases))
		for _, c := range existCases {
			existResponse = append(existResponse, c.SQL)
		}
		assistantMessage, err := json.Marshal(simplePromptResponse{
			Queries: existResponse,
		})
		// should never happen
		if err != nil {
			logger.Global.Info("failed to marshal exist response", zap.Error(err))
			return nil
		}
		messages = append(messages, openai.AssistantMessage(string(assistantMessage)))
	}
	messages = append(messages, openai.UserMessage(fmt.Sprintf(userPromptTemplate, count, group)))

	return messages
}

// Unmarshal implements PromptGenerator.Unmarshal
func (g *dmlPromptGenerator) Unmarshal(response string) []testcase.Case {
	var resp simplePromptResponse
	err := json.Unmarshal([]byte(response), &resp)
	if err != nil {
		logger.Global.Error("failed to unmarshal dml prompt response", zap.Error(err), zap.String("response", response))
		return nil
	}

	cases := make([]testcase.Case, 0, len(resp.Queries))
	for _, q := range resp.Queries {
		cases = append(cases, testcase.Case{
			SQL: q,
		})
	}

	return cases
}

func init() {
	registerPromptGenerator(&dmlPromptGenerator{})
}
