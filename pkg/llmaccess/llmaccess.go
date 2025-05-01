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

package llmaccess

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
)

const (
	OpenAI = "openai"
)

type LLMAccessor interface {
	// ChatCompletion calls the specified LLM to complete the chat based on input prompt.
	ChatCompletion(platform, model, prompt string) (response string, err error)

	LoadLLMPlatform() error

	LoadLLMModel() error

	AlterPlatform(sctx sessionctx.Context, platform string, options []string, values []any) error

	AlterModel(sctx sessionctx.Context, name string, options []string, values []any) error

	CreateModel(sctx sessionctx.Context, name string, options []string, values []any) error

	DropModel(sctx sessionctx.Context, name string) error
}

type llmAccessorImpl struct {
	sPool util.DestroyableSessionPool

	platforms atomic.Value
	models    atomic.Value
}

func NewLLMAccessor(sPool util.DestroyableSessionPool) LLMAccessor {
	return &llmAccessorImpl{sPool: sPool}
}

func (llm *llmAccessorImpl) CreateModel(sctx sessionctx.Context, name string, options []string, values []any) error {
	requiredOptions := []string{"PLATFORM", "MODEL", "STATUS"}
	for _, opt := range requiredOptions {
		ok := false
		for i := range options {
			if strings.ToUpper(opt) == strings.ToUpper(options[i]) {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("missing required option %s", opt)
		}
	}
	if err := llm.validateModelOptions(options, values); err != nil {
		return err
	}

	var userName string
	if sctx.GetSessionVars().User != nil { // might be nil if in test
		userName = sctx.GetSessionVars().User.AuthUsername
	}
	columnNames := append(options, "name", "user")
	columnValues := append(values, name, userName)
	n := len(columnNames)

	createStmt := "insert into mysql.llm_model (" + strings.Join(columnNames, ",") +
		") values (" + strings.Repeat("%?,", n-1) + "%?)"

	if err := callWithSCtx(llm.sPool, true, func(tmpCtx sessionctx.Context) error {
		_, err := exec(tmpCtx, createStmt, columnValues...)
		sctx.GetSessionVars().StmtCtx.SetAffectedRows(tmpCtx.GetSessionVars().StmtCtx.AffectedRows())
		return err
	}); err != nil {
		return err
	}
	return llm.LoadLLMModel()
}

func (llm *llmAccessorImpl) AlterModel(sctx sessionctx.Context, name string, options []string, values []any) error {
	if err := llm.validateModelOptions(options, values); err != nil {
		return err
	}
	columnSet := make([]string, 0, len(options))
	for i := range options {
		columnSet = append(columnSet, "`"+options[i]+"` = %?")
	}
	updateStmt := "update mysql.llm_model set " + strings.Join(columnSet, ", ") + " where `name` = %?"
	if err := callWithSCtx(llm.sPool, true, func(tmpCtx sessionctx.Context) error {
		args := append(values, name)
		_, err := exec(tmpCtx, updateStmt, args...)
		sctx.GetSessionVars().StmtCtx.SetAffectedRows(tmpCtx.GetSessionVars().StmtCtx.AffectedRows())
		return err
	}); err != nil {
		return err
	}
	return llm.LoadLLMModel()
}

func (llm *llmAccessorImpl) DropModel(sctx sessionctx.Context, name string) error {
	deleteStmt := "delete from mysql.llm_model where `name` = %?"
	if err := callWithSCtx(llm.sPool, true, func(tmpCtx sessionctx.Context) error {
		_, err := exec(tmpCtx, deleteStmt, name)
		sctx.GetSessionVars().StmtCtx.SetAffectedRows(tmpCtx.GetSessionVars().StmtCtx.AffectedRows())
		return err
	}); err != nil {
		return err
	}
	return llm.LoadLLMModel()
}

func (llm *llmAccessorImpl) validateModelOptions(options []string, values []any) error {
	for i := range options {
		v := values[i]
		switch strings.ToUpper(options[i]) {
		case "PLATFORM", "API_VERSION", "MODEL", "REGION", "EXTRAS":
			if !isStr(v) {
				return fmt.Errorf("invalid value for %s: %v", options[i], v)
			}
		case "STATUS":
			if !isStr(v) {
				return fmt.Errorf("invalid value for %s: %v", options[i], v)
			}
			str := strings.ToUpper(v.(string))
			if str != "ENABLED" && str != "DISABLED" {
				return fmt.Errorf("invalid value for %s: %v", options[i], v)
			}
		case "MAX_TOKENS":
			if !isInt(v) {
				return fmt.Errorf("invalid value for %s: %v", options[i], v)
			}
		default:
			return fmt.Errorf("unsupported option: %s", options[i])
		}
	}
	return nil
}

func (llm *llmAccessorImpl) AlterPlatform(sctx sessionctx.Context, platform string, options []string, values []any) error {
	platform, err := formatPlatform(platform)
	if err != nil {
		return err
	}
	if err := llm.validatePlatformOptions(options, values); err != nil {
		return err
	}

	columnSet := make([]string, 0, len(options))
	for i := range options {
		columnSet = append(columnSet, "`"+options[i]+"` = %?")
	}
	updateStmt := "update mysql.llm_platform set " + strings.Join(columnSet, ", ") + " where `name` = %?"
	if err := callWithSCtx(llm.sPool, true, func(tmpCtx sessionctx.Context) error {
		args := append(values, platform)
		_, err := exec(tmpCtx, updateStmt, args...)
		sctx.GetSessionVars().StmtCtx.SetAffectedRows(tmpCtx.GetSessionVars().StmtCtx.AffectedRows())
		return err
	}); err != nil {
		return err
	}
	return llm.LoadLLMPlatform()
}

func (llm *llmAccessorImpl) validatePlatformOptions(options []string, values []any) error {
	for i := range options {
		v := values[i]
		switch strings.ToUpper(options[i]) {
		case "API_KEY":
			if !isStr(v) {
				return fmt.Errorf("invalid value for %s: %v", options[i], v)
			}
		case "STATUS":
			if !isStr(v) {
				return fmt.Errorf("invalid value for %s: %v", options[i], v)
			}
			str := strings.ToUpper(v.(string))
			if str != "ENABLED" && str != "DISABLED" {
				return fmt.Errorf("invalid value for %s: %v", options[i], v)
			}
		case "MAX_TOKENS":
			if !isInt(v) {
				return fmt.Errorf("invalid value for %s: %v", options[i], v)
			}
		default:
			return fmt.Errorf("unsupported option: %s", options[i])
		}
	}
	return nil
}

// ChatCompletion calls the specified LLM to complete the chat based on input prompt.
func (llm *llmAccessorImpl) ChatCompletion(platform, model, prompt string) (response string, err error) {
	platform, err = formatPlatform(platform)
	if err != nil {
		return "", err
	}

	switch platform {
	case OpenAI:
		return llm.chatCompletionOpenAI(model, prompt, "")
	default:
		return "", fmt.Errorf("unsupported platform: %d", platform)
	}
}

func (*llmAccessorImpl) chatCompletionOpenAI(model, prompt, key string) (response string, err error) {
	if key == "" {
		key, _ = os.LookupEnv("OPENAI_API_KEY")
	}
	client := openai.NewClient(
		option.WithAPIKey(key), // defaults to os.LookupEnv("OPENAI_API_KEY")
	)
	chatCompletion, err := client.Chat.Completions.New(context.TODO(), openai.ChatCompletionNewParams{
		Messages: openai.F([]openai.ChatCompletionMessageParamUnion{
			openai.UserMessage(prompt),
		}),
		Model: openai.F(model),
	})
	if err != nil {
		llmLogger().Error("failed to call OpenAI",
			zap.String("platform", OpenAI), zap.String("model", model),
			zap.String("prompt", prompt), zap.Error(err))
		return "", err
	}
	return chatCompletion.Choices[0].Message.Content, nil
}

func (llm *llmAccessorImpl) LoadLLMPlatform() error {
	stmt := `select name, base_url, host, auth, source, description, api_key,
		default_model, max_tokens, timeout, status from mysql.llm_platform`
	return callWithSCtx(llm.sPool, false, func(sctx sessionctx.Context) error {
		rows, _, err := execRows(sctx, stmt)
		if err != nil {
			return err
		}
		platforms := make([]*platform, 0, len(rows))
		for _, row := range rows {
			p := &platform{
				Name:         row.GetString(0),
				BaseURL:      row.GetString(1),
				Host:         row.GetString(2),
				Auth:         row.GetString(3),
				Source:       row.GetString(4),
				Desc:         row.GetString(5),
				APIKey:       row.GetString(6),
				DefaultModel: row.GetString(7),
				MaxTokens:    row.GetInt64(8),
				Status:       row.GetString(9),
			}
			platforms = append(platforms, p)
		}
		llm.platforms.Store(platforms)
		return nil
	})
}

func (llm *llmAccessorImpl) LoadLLMModel() error {
	stmt := `select user, name, platform, api_version, model, region,
		max_tokens, status, extras, comment from mysql.llm_model`

	return callWithSCtx(llm.sPool, false, func(sctx sessionctx.Context) error {
		rows, _, err := execRows(sctx, stmt)
		if err != nil {
			return err
		}
		models := make([]*model, 0, len(rows))
		for _, row := range rows {
			m := &model{
				User:       row.GetString(0),
				Name:       row.GetString(1),
				Platform:   row.GetString(2),
				APIVersion: row.GetString(3),
				Model:      row.GetString(4),
				Region:     row.GetString(5),
				MaxTokens:  row.GetInt64(6),
				Status:     row.GetString(7),
				Comment:    row.GetString(9),
			}
			models = append(models, m)
		}
		llm.models.Store(models)
		return nil
	})
}

type platform struct {
	Name         string
	BaseURL      string
	Host         string
	Auth         string
	Source       string
	Desc         string
	APIKey       string
	DefaultModel string
	MaxTokens    int64
	Status       string
	Extras       string // JSON
}

type model struct {
	User       string
	Name       string
	Platform   string
	APIVersion string
	Model      string
	Region     string
	MaxTokens  int64
	Status     string
	Comment    string
}

func formatPlatform(platform string) (string, error) {
	switch strings.ToLower(platform) {
	case OpenAI:
		return OpenAI, nil
	}
	return "", fmt.Errorf("unsupported platform: %s", platform)
}

func isStr(v any) bool {
	_, ok := v.(string)
	return ok
}

func isInt(v any) bool {
	_, ok1 := v.(int)
	_, ok2 := v.(int8)
	_, ok3 := v.(int16)
	_, ok4 := v.(int32)
	_, ok5 := v.(int64)
	return ok1 || ok2 || ok3 || ok4 || ok5
}

func isFloat(v any) bool {
	_, ok1 := v.(float64)
	_, ok2 := v.(float32)
	return ok1 || ok2
}
