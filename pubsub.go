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

func (c *CheckSubscriptionPullPermission) Run(dctx *selfdiagnose.Context, result *selfdiagnose.Result) {
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
	ctx := context.Background()
	if c.BasicTask.Timeout() > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.BasicTask.Timeout())
		defer cancel()
	}
	subset, err := sub.IAM().TestPermissions(ctx, tocheck)
	if err != nil {
		result.Passed = false
		result.Reason = fmt.Errorf("could test permissions:%w", err)
		return
	}
	result.Passed = len(subset) == len(tocheck)
	result.Reason = fmt.Sprintf("This service account has the following permissions: %v on subscription: %s", subset, c.SubscriptionName)
}
