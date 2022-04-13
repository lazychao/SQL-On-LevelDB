package main

import (
	"os"

	"github.com/jedib0t/go-pretty/v6/table"
)

func main() {

	a := table.NewWriter()
	a.SetStyle(table.StyleColoredBright)
	a.SetOutputMirror(os.Stdout)
	a.AppendHeader(table.Row{"#", "First Name", "Last Name", "Salary"})

	a.Render()
	a.ResetHeaders()
	a.AppendRows([]table.Row{
		{1, "Arya", "Stark", 3000},
		{20, "Jon", "Snow", 2000, "You know nothing, Jon Snow!"},
	})
	a.AppendRow([]interface{}{300, "Tyrion", "Lannister", 5000})
	a.Render()
	a.ResetRows()
	a.AppendFooter(table.Row{"", "", "Total", 10000})
	a.Render()

}
