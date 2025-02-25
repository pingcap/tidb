package bindinfo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/pingcap/tidb/pkg/sessionctx"
)

func (h *globalBindingHandle) LLM(autoBindings []*AutoBindingInfo) {
	bindingPlans := make([]string, 0, len(autoBindings))
	for i, autoBinding := range autoBindings {
		planText, err := h.autoBindingPlanText(autoBinding)
		if err != nil {
			fmt.Println("err ", err)
			return
		}
		bindingPlans = append(bindingPlans, fmt.Sprintf("%d. %v\n%v\n", i, autoBinding.BindSQL, planText))
	}

	promptPattern := `You are a TiDB expert.
You are going to help me decide which hint should be used for a specified SQL.
Be careful with the escape characters.

The SQL is "%v".

Here are these hinted SQLs and their plans:
%v

Please tell me which one is the best, and the reason.
The reason should be concise, not more than 50 words.
Please return a valid JSON object with the key "best_number" and "reason".
IMPORTANT: Don't put anything else in the response.
Here is an example of output JSON:
    {"best_number": 2, "reason": "This hint can utilize an index to filter unnecessary data"}
`
	prompt := fmt.Sprintf(promptPattern, autoBindings[0].OriginalSQL, strings.Join(bindingSQLs, "\n"))

	fmt.Println("--------------------- prompt ------------------------------")
	fmt.Println(prompt)
	fmt.Println("--------------------- prompt ------------------------------")

	resp, ok, err := CallLLM("", "https://api.deepseek.com/chat/completions", prompt)
	if err != nil {
		fmt.Println("err ", err)
		return
	}
	if ok && resp != "" {
		fmt.Println("=======================================================")
		fmt.Println(resp)
		fmt.Println("=======================================================")
	}
}

func (h *globalBindingHandle) autoBindingPlanText(autoBinding *AutoBindingInfo) (string, error) {
	var planText string
	err := h.callWithSCtx(false, func(sctx sessionctx.Context) error {
		rows, _, err := execRows(sctx, "explain "+autoBinding.BindSQL)
		if err != nil {
			return err
		}
		/*
			+-----------------------+---------+-----------+---------------+--------------------------------+
			| id                    | estRows | task      | access object | operator info                  |
			+-----------------------+---------+-----------+---------------+--------------------------------+
			| TableReader_5         | 5.00    | root      |               | data:TableFullScan_4           |
			| └─TableFullScan_4     | 5.00    | cop[tikv] | table:t       | keep order:false, stats:pseudo |
			+-----------------------+---------+-----------+---------------+--------------------------------+
		*/
		for _, row := range rows {
			planText += fmt.Sprintf("%v\t%v\t%v\t%v\t%v\n",
				row.GetString(0), row.GetString(1), row.GetString(2),
				row.GetString(3), row.GetString(4))
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return planText, nil
}

type ChatRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatResponse struct {
	Choices []struct {
		Message Message `json:"message"`
	} `json:"choices"`
}

func CallLLM(apiKey, apiURL, msg string) (respMsg string, ok bool, err error) {
	requestBody := ChatRequest{
		Model: "deepseek-reasoner",
		Messages: []Message{
			{
				Role:    "user",
				Content: msg,
			},
		},
	}
	reqBytes, err := json.Marshal(requestBody)
	if err != nil {
		return "", false, err
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return "", false, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", false, err
	}

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("API FAIL, status code: %d, resp: %v", resp.StatusCode, string(body))
		return "", false, err
	}

	var response ChatResponse
	if err = json.Unmarshal(body, &response); err != nil {
		return "", false, err
	}

	if len(response.Choices) > 0 {
		respMsg = response.Choices[0].Message.Content
	} else {
		return "", false, nil
	}
	return respMsg, true, nil
}
