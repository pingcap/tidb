package bindinfo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func (h *globalBindingHandle) LLM(autoBindings []*AutoBindingInfo) {
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
