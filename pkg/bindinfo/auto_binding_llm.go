package bindinfo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func (h *globalBindingHandle) LLM(autoBindings []*AutoBindingInfo) {
	bindingSQLs := make([]string, 0, len(autoBindings))
	for i, autoBinding := range autoBindings {
		bindingSQLs = append(bindingSQLs, fmt.Sprintf("%d. %v", i, autoBinding.BindSQL))
	}

	promptPattern := `You are a TiDB expert and now you are going to help me decide which hint (or binding) should be used for the following SQL.
The SQL is "%v".

And here are these SQLs with hints:
%v
`
	prompt := fmt.Sprintf(promptPattern, autoBindings[0].OriginalSQL, strings.Join(bindingSQLs, "\n"))

	fmt.Println("--------------------- prompt ------------------------------")
	fmt.Println(prompt)
	fmt.Println("--------------------- prompt ------------------------------")

	resp, ok, err := CallLLM("", "https://api.deepseek.com/chat/completions", prompt)
	if err != nil {
		fmt.Println("err")
		return
	}
	if ok && resp != "" {
		fmt.Println("=======================================================")
		fmt.Println(resp)
		fmt.Println("=======================================================")
	}
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
		Model: "deepseek-chat",
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
