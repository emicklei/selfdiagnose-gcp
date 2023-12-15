package gcptask

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/pubsub"
	"github.com/emicklei/go-selfdiagnose"
)

type CheckSubscriptionPullPermission struct {
	selfdiagnose.BasicTask
	SubscriptionName string // full qualified name , e.g. projects/.../subscriptions/....
}

func (c *CheckSubscriptionPullPermission) Run(ctx *selfdiagnose.Context, result *selfdiagnose.Result) {
	parts := strings.Split(c.SubscriptionName, "/")
	projectID := parts[1]
	name := parts[3]
	client, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		result.Passed = false
		result.Reason = fmt.Errorf("could read create client:%w", err)
		return
	}
	sub := client.Subscription(name)
	tocheck := []string{"pubsub.subscriptions.consume"}
	subset, err := sub.IAM().TestPermissions(context.Background(), tocheck)
	if err != nil {
		result.Passed = false
		result.Reason = fmt.Errorf("could test permissions:%w", err)
		return
	}
	result.Passed = len(subset) == len(tocheck)
	result.Reason = fmt.Sprintf("This service account has the following permissions: %v on subscription: %s", subset, c.SubscriptionName)
}
