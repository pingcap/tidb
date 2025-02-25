package bindinfo

import (
	"context"
	"fmt"
	"log"

	"github.com/sashabaranov/go-openai"
)

func (h *globalBindingHandle) LLM(autoBindings []*AutoBindingInfo) {
	apiKey := "YOUR_API_KEY"
	client := openai.NewClient(apiKey)
	ctx := context.Background()

	req := openai.ChatCompletionRequest{
		Model: openai.GPT3Dot5Turbo,
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleUser,
				Content: "推荐一部好看的科幻电影",
			},
		},
	}

	resp, err := client.CreateChatCompletion(ctx, req)
	if err != nil {
		log.Fatalf("创建聊天完成时出错: %v", err)
	}

	fmt.Println(resp.Choices[0].Message.Content)
}
