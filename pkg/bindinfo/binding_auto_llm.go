package bindinfo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
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

	resp, err := CallLLM(os.Getenv("LLM_KEY"), os.Getenv("LLM_URL"), prompt)
	if err != nil {
		fmt.Println("err ", err)
		return
	}

	fmt.Println("=================== RESP BODY =========================")
	fmt.Println(resp)
	fmt.Println("=================== RESP BODY =========================")

	if strings.HasPrefix(resp, "```json") {
		resp = strings.TrimPrefix(resp, "```json")
		resp = strings.TrimSuffix(resp, "```")
	}

	r := new(LLMRecommendation)
	err = json.Unmarshal([]byte(resp), r)
	if err != nil {
		fmt.Println("unmarshal error ", err)
		return
	}

	if r.Number < 0 || r.Number >= len(bindingPlans) {
		fmt.Println(errors.New("invalid result number"))
	}

	bindingPlans[r.Number].Recommend = "YES (from LLM)"
	bindingPlans[r.Number].Reason = r.Reason
	return
}

type LLMRecommendation struct {
	Number int    `json:"number"`
	Reason string `json:"reason"`
}

func CallLLM(apiKey, apiURL, msg string) (respMsg string, err error) {
	requestBody := ChatRequest{
		Model: "deepseek-chat",
		Messages: []Message{
			{
				Role:    "user",
				Content: msg,
			},
		},
		Stream: false,
	}
	reqBytes, err := json.Marshal(requestBody)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("API FAIL, status code: %d, resp: %v", resp.StatusCode, string(body))
		return "", err
	}

	var response ChatResponse
	if err = json.Unmarshal(body, &response); err != nil {
		return "", err
	}

	if len(response.Choices) > 0 {
		respMsg = response.Choices[0].Message.Content
	} else {
		return "", nil
	}
	return respMsg, nil
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatRequest struct {
	Model    string    `json:"model"`
	Stream   bool      `json:"stream"`
	Messages []Message `json:"messages"`
}

type Choice struct {
	Message Message `json:"message"`
}

type ChatResponse struct {
	Choices []Choice `json:"choices"`
}
