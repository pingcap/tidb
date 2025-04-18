# LLMTest

It's a tool to help TiDB developers to generate test cases through LLM for any features, and run the test cases in TiDB.

## Generate

The `LLMTest` uses different "prompt generator" to generate the tests for different features. For example, the `generator/expression.go` includes the expression prompt generator to generate the test cases for expression module.

To generate test cases with a specific prompt generator, you can use the following command:

```bash
./llmtest generate --openai_base_url https://openai.base.url --openai_model deepseek/deepseek-r1 --openai_token XXXXX --parallel 20 --prompt_generator expression --test_count 10
```

Replace the `prompt_generator` with the specific prompt generator you want to use.

## Add a new prompt generator

To add a new prompt generator, you need to implement the `PromptGenerator` interface in the `generator/prompt.go` file. The `PromptGenerator` interface includes the following methods:

```go
// PromptGenerator is the interface for prompt generator.
type PromptGenerator interface {
	Name() string
	Groups() []string

	GeneratePrompt(group string, count int, existCases []*testcase.Case) []openai.ChatCompletionMessageParamUnion
	Unmarshal(response string) []testcase.Case
}
```

The `Name` method returns the name of the prompt generator. The `Groups` method returns the sub-classes of the prompt generator. The `GeneratePrompt` method generates the prompt for the test cases. The `Unmarshal` method unmarshals the response from the OpenAI API to the test cases.

The `Groups()` method is used to classify the test cases. For example, the `expression` prompt generator has the following groups:

```go
[]string{"+", "-" ....}
```

## Verify

To verify the generated test cases, you can use the following command:

```bash
./llmtest verify --mysql_dsn "root:123456@tcp(127.0.0.1:3306)/test" --tidb_dsn "root@tcp(127.0.0.1:4000)/test" --prompt_generator expression
```

It'll execute the generated test cases in the TiDB cluster and MySQL to verify the results.
