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

func (c *CheckBigQueryTablePermissions) Run(ctx *selfdiagnose.Context, result *selfdiagnose.Result) {
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
	bqTable := client.Dataset(dataset).Table(table)
	_, err = bqTable.Metadata(context.Background())
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
	subset, err := bqTable.IAM().TestPermissions(context.Background(), tocheck)
	if err != nil {
		result.Passed = false
		result.Reason = fmt.Errorf("could read permissions:%w", err)
		return
	}
	result.Passed = len(subset) == len(tocheck)
	result.Reason = fmt.Sprintf("This service account has the following permissions: %v on table: %s", subset, c.TableID)
}
