package cmd

import (
	"dobbbb/tool/es"
	"encoding/json"
	"github.com/spf13/cobra"
	"log"
	"os"
	"time"
)

// compareCmd represents the compare command
var compareCmd = &cobra.Command{
	Use:   "compare",
	Short: "Compare the data of the two indexes",
	Long: `Compare the data of the two indexes. For example:
Full compare:
	./tool compare \
	-a http://127.0.0.1:9200 -u username -p password -i index \
	-H http://0.0.0.0:9200 -U username1 -P password1 -I index1 \
	-s 4000 \
	-S 8
Specify condition compare:
	./tool compare \
	-a http://127.0.0.1:9200 -u username -p password -i index \
	-H http://0.0.0.0:9200 -U username1 -P password1 -I index1 \
	-s 4000 \
	-S 8 \
	-q '{"query":{"terms":{"id":["1715478400352693852"]}}}'
`,
	Run: func(cmd *cobra.Command, args []string) {
		cc := es.CompareContext{}
		cc.SrcHost, _ = cmd.Flags().GetString("srcHost")
		cc.SrcUserName, _ = cmd.Flags().GetString("srcUserName")
		cc.SrcPassword, _ = cmd.Flags().GetString("srcPassword")
		cc.SrcIndex, _ = cmd.Flags().GetString("srcIndex")
		cc.Query, _ = cmd.Flags().GetString("query")
		cc.DstHost, _ = cmd.Flags().GetString("dstHost")
		cc.DstUserName, _ = cmd.Flags().GetString("dstUserName")
		cc.DstPassword, _ = cmd.Flags().GetString("dstPassword")
		cc.DstIndex, _ = cmd.Flags().GetString("dstIndex")
		cc.BatchSize, _ = cmd.Flags().GetInt("size")
		cc.Slices, _ = cmd.Flags().GetInt("slices")
		cc.ConcurNum, _ = cmd.Flags().GetInt("concurNum")
		cc.Field, _ = cmd.Flags().GetString("field")
		cc.IsRepair, _ = cmd.Flags().GetBool("repair")
		cc.Init()
		start := time.Now().Unix()
		if cc.Slices > 1 {
			cc.ScrollSliceCompare()
		} else if cc.ConcurNum > 1 {
			cc.CustomConcurrencyCompare()
		} else {
			cc.Compare(nil, cc.Query)
		}
		if cc.IsRepair {
			cc.Repair()
		}
		log.Printf("totol cast: %ds", time.Now().Unix()-start)

		if len(cc.DiffIds) > 0 {
			if ids, err := json.Marshal(cc.DiffIds); err == nil {
				if err := os.WriteFile("diffIds", ids, 0644); err != nil {
					log.Fatal(err)
				}
			} else {
				log.Fatal(err)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(compareCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// compareCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	compareCmd.Flags().StringP("srcHost", "a", "", "Source ES host: http//ip:port")
	compareCmd.Flags().StringP("srcUserName", "u", "", "Source ES userName")
	compareCmd.Flags().StringP("srcPassword", "p", "", "Source ES password")
	compareCmd.Flags().StringP("srcIndex", "i", "", "Source ES index")
	compareCmd.Flags().StringP("query", "q", "", "Query body")

	compareCmd.Flags().StringP("dstHost", "A", "", "Destination ES host: http//ip:port, default same as srcHost.")
	compareCmd.Flags().StringP("dstUserName", "U", "", "Destination ES userName, default same as srcUserName.")
	compareCmd.Flags().StringP("dstPassword", "P", "", "Destination ES password, default same as srcPassword.")
	compareCmd.Flags().StringP("dstIndex", "I", "", "Destination ES index, It should be different from srcIndex when dstHost is same as srcHost.")

	compareCmd.Flags().IntP("size", "s", 1000, "Batch size")
	compareCmd.Flags().IntP("slices", "S", 1, "Scroll slices, suggest to use the same number of shards")
	compareCmd.Flags().IntP("concurNum", "c", 1, "Number of custom concurrency queries")
	compareCmd.Flags().StringP("field", "f", "order_time", "Custom concurrency field")

	compareCmd.Flags().BoolP("repair", "r", false, "Auto repair")

	_ = compareCmd.MarkFlagRequired("srcHost")
	_ = compareCmd.MarkFlagRequired("srcIndex")
}
