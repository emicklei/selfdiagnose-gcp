package gcptask

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/emicklei/go-selfdiagnose"
)

type CheckBigQueryTablePermissions struct {
	selfdiagnose.BasicTask
	TableID      string // full qualified table id, e.g proj.dataset.table
	IsReadable   bool
	IsUpdateable bool
}

func (c *CheckBigQueryTablePermissions) Run(dctx *selfdiagnose.Context, result *selfdiagnose.Result) {
	parts := strings.Split(c.TableID, ".")
	project := parts[0]
	dataset := parts[1]
	table := parts[2]
	client, err := bigquery.NewClient(context.Background(), project)
	if err != nil {
		result.Passed = false
		result.Reason = fmt.Errorf("could not create bigquery client:%w", err)
		return
	}
	ctx := context.Background()
	if c.BasicTask.Timeout() > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.BasicTask.Timeout())
		defer cancel()
	}
	bqTable := client.Dataset(dataset).Table(table)
	_, err = bqTable.Metadata(ctx)
	if err != nil {
		result.Passed = false
		result.Reason = fmt.Errorf("could read metadata:%w", err)
		return
	}
	tocheck := []string{}
	if c.IsReadable {
		tocheck = append(tocheck, "bigquery.tables.read")
	}
	if c.IsUpdateable {
		tocheck = append(tocheck, "bigquery.tables.update")
	}
	subset, err := bqTable.IAM().TestPermissions(ctx, tocheck)
	if err != nil {
		result.Passed = false
		result.Reason = fmt.Errorf("could read permissions:%w", err)
		return
	}
	result.Passed = len(subset) == len(tocheck)
	result.Reason = fmt.Sprintf("This service account has the following permissions: %v on table: %s", subset, c.TableID)
}
