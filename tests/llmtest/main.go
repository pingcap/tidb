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

package main

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/tests/llmtest/generator"
	"github.com/pingcap/tidb/tests/llmtest/logger"
	"github.com/pingcap/tidb/tests/llmtest/testcase"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func main() {
	var rootCmd = &cobra.Command{Use: "llmtest"}

	generateCmd := createGenerateCmd()
	verifyCmd := createVerifyCmd()

	rootCmd.AddCommand(generateCmd, verifyCmd)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func createGenerateCmd() *cobra.Command {
	var (
		openaiToken         string
		openaiBaseURL       string
		openaiModel         string
		promptGeneratorName string
		testCount           int
		generateParallism   int
	)

	var generateCmd = &cobra.Command{
		Use:   "generate",
		Short: "Generate something using OpenAI",
		Run: func(cmd *cobra.Command, args []string) {
			promptGenerator := generator.GetPromptGenerator(promptGeneratorName)
			if promptGenerator == nil {
				logger.Global.Info("Unknown prompt generator", zap.String("name", promptGeneratorName))
				os.Exit(1)
			}

			caseManager, err := testcase.Open("testdata/" + promptGeneratorName + ".json")
			if err != nil {
				logger.Global.Error("Failed to open test case", zap.Error(err))
				os.Exit(1)
			}

			caseGenerator := generator.New(
				caseManager, generateParallism,
				openaiToken, openaiBaseURL, openaiModel, promptGenerator, testCount)
			caseGenerator.Run()
			caseGenerator.Wait()

			err = caseManager.Save()
			if err != nil {
				logger.Global.Error("Failed to save test case", zap.Error(err))
				os.Exit(1)
			}
		},
	}

	generateCmd.Flags().StringVar(&openaiToken, "openai_token", "", "OpenAI token")
	generateCmd.Flags().StringVar(&openaiBaseURL, "openai_base_url", "", "OpenAI base URL")
	generateCmd.Flags().StringVar(&openaiModel, "openai_model", "", "OpenAI model")
	generateCmd.Flags().StringVar(&promptGeneratorName, "prompt_generator", "", "Prompt generator")
	generateCmd.Flags().IntVar(&testCount, "test_count", 0, "Test count")
	generateCmd.Flags().IntVar(&generateParallism, "parallel", 20, "Generate parallism")
	return generateCmd
}

func createVerifyCmd() *cobra.Command {
	var (
		promptGeneratorName string
		tidbDSN             string
		mysqlDSN            string
		recheckPassed       bool
	)

	var verifyCmd = &cobra.Command{
		Use:   "verify",
		Short: "Verify something using TiDB and MySQL",
		Run: func(cmd *cobra.Command, args []string) {
			caseManager, err := testcase.Open("testdata/" + promptGeneratorName + ".json")
			if err != nil {
				logger.Global.Error("Failed to open test case", zap.Error(err))
				os.Exit(1)
			}

			tidb, err := sql.Open("mysql", unifyDSN(tidbDSN))
			if err != nil {
				logger.Global.Error("Failed to open TiDB", zap.Error(err))
				os.Exit(1)
			}
			defer tidb.Close()

			mysql, err := sql.Open("mysql", unifyDSN(mysqlDSN))
			if err != nil {
				logger.Global.Error("Failed to open MySQL", zap.Error(err))
				os.Exit(1)
			}

			caseManager.RunABTest(tidb, mysql, recheckPassed)
			err = caseManager.Save()
			if err != nil {
				logger.Global.Error("Failed to save test case", zap.Error(err))
				os.Exit(1)
			}
		},
	}
	verifyCmd.Flags().StringVar(&promptGeneratorName, "prompt_generator", "", "Prompt generator")
	verifyCmd.Flags().StringVar(&tidbDSN, "tidb_dsn", "", "TiDB DSN")
	verifyCmd.Flags().StringVar(&mysqlDSN, "mysql_dsn", "", "MySQL DSN")
	verifyCmd.Flags().BoolVar(&recheckPassed, "recheck_passed", false, "Recheck passed cases")

	return verifyCmd
}

func unifyDSN(dsn string) string {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		logger.Global.Error("Failed to parse DSN", zap.Error(err))
		os.Exit(1)
	}

	// TODO: allow to configure different collation
	cfg.Collation = "utf8mb4_bin"

	return cfg.FormatDSN()
}
