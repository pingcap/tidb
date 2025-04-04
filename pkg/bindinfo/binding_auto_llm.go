package bindinfo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

// FillRecommendationViaLLM fills the recommendation field for each binding plan via LLM.
func FillRecommendationViaLLM(bindingPlans []*BindingPlanInfo) {
	promptPattern := `You are a TiDB expert.
You are going to help me decide which hint should be used for a specified SQL.
Be careful with the escape characters.
Be careful that estRows might not be accurate.
You can take at most 20 seconds to think of this.
The SQL is "%v".
Here are these hinted SQLs and their plans:
%v
Please tell me which one is the best, and the reason.
The reason should be concise, not more than 200 words.
Please return a valid JSON object with the key "number" and "reason".
IMPORTANT: Don't put anything else in the response and return the raw json data directly, remove "` + "```" + `json".
Here is an example of output JSON:
    {"number": 2, "reason": "xxxxxxxxxxxxxxxxxxx"}`
	bindingPlanText := make([]string, 0, len(bindingPlans))
	for i, p := range bindingPlans {
		bindingPlanText = append(bindingPlanText, fmt.Sprintf("%d. %v\n%v\n", i, p.BindSQL, p.Plan))
	}

	prompt := fmt.Sprintf(promptPattern, bindingPlans[0].OriginalSQL, strings.Join(bindingPlanText, "\n"))

	fmt.Println("--------------------- prompt ------------------------------")
	fmt.Println(prompt)
	fmt.Println("--------------------- prompt ------------------------------")

}

func CallLLM(apiKey, prompt string) (string, error) {
	if apiKey == "" {
		return "", errors.New("not set OPENAI_API_KEY")
	}

	fmt.Println("------------------------------------- call LLM -------------------------------------")
	fmt.Println("apiKey: ", apiKey)
	fmt.Println("prompt: ", prompt)
	fmt.Println("-------------------------------------------------------------------------------------")

	url := "https://api.openai.com/v1/chat/completions"

	requestBody := ChatRequest{
		Model: "gpt-3.5-turbo", // or "gpt-4"
		Messages: []Message{
			{Role: "user", Content: prompt},
		},
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		panic(err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{
		Timeout: 120 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		fmt.Printf("Error: %s\n", body)
		return "", fmt.Errorf("Error: %s\n", body)
	}

	var chatResp ChatResponse
	err = json.Unmarshal(body, &chatResp)
	if err != nil {
		fmt.Printf("Error: %s\n", body)
	}

	return chatResp.Choices[0].Message.Content, nil
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
}

type Choice struct {
	Message Message `json:"message"`
}

type ChatResponse struct {
	Choices []Choice `json:"choices"`
}
