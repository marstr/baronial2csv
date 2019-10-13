package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/marstr/collection"
	"github.com/marstr/envelopes"
	"github.com/marstr/envelopes/persist"
)

type transactionEnumerable struct {
	Head envelopes.ID
	persist.Loader
}

func (te transactionEnumerable) Enumerate(cancel <-chan struct{}) collection.Enumerator {
	current := te.Head

	results := make(chan interface{})

	go func(){
		defer close(results)

		var element envelopes.Transaction
		for !current.Equal(envelopes.ID{}){
			err := te.Loader.Load(context.Background(), current, &element)
			if err != nil {
				return
			}

			select {
			case results <- element:
				// Intentionally Left Blank
			case <-cancel:
				return
			}
		}
	}()

	return results
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "please provide exactly one argument, the root directory of your baronial repo.")
		return
	}

	fs := persist.FileSystem{
		Root: os.Args[1],
	}

	te := transactionEnumerable{
		Loader: persist.DefaultLoader{
			Fetcher: fs,
		},
	}
	brokenUp := transform(te)

	output := csv.NewWriter(os.Stdout)
	defer output.Flush()

	output.Write(getTitleRecord())
	for impRecord := range brokenUp.Enumerate(nil) {
		if impRecord == nil {
			continue
		}

		cast := impRecord.([]string)
		output.Write(cast)
	}
}

func transform(transactions transactionEnumerable) collection.Enumerable {
	var previous *envelopes.Transaction
	return collection.SelectMany(transactions, func(t interface{}) collection.Enumerator {
		cast := t.(envelopes.Transaction)

		var retval collection.Enumerator

		if previous != nil {
			imp := cast.State.Subtract(*previous.State)


			results := make(chan interface{}, len(imp.Accounts) + countBudgets(imp.Budget))
			retval = results

			go func(){
				defer close(results)

				for acc, amount := range imp.Accounts {
					getDataRecord(amount, previous.ID(), acc, "Account", previous.Time)
				}
			}()
		}

		previous = &cast
		return retval
	})
}

func countBudgets(subject *envelopes.Budget) (retval int) {
	if subject == nil {
		return
	}

	retval += 1

	for _, child := range subject.Children {
		retval += countBudgets(child)
	}

	return
}

func getTitleRecord() []string {
	return []string{
		"Transaction",
		"Time",
		"Amount",
		"Entity",
		"Entity Type",
	}
}

func getDataRecord(amount envelopes.Balance, id envelopes.ID, entity, entityType string, time time.Time) []string {
	return []string{
		id.String(),
		time.String(),
		amount.String(),
		entity,
		entityType,
	}
}
