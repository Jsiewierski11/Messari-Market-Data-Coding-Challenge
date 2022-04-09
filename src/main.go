package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"encoding/json"
	"log"
	"bufio"
)

type Trade struct {
	ID     int     `json:"id"`
	Market int     `json:"market"`
	Price  float64 `json:"price"`
	Volume float64 `json:"volume"`
	IsBuy  bool    `json:"is_buy"`
}

// simple test to read input and parse json
func test_example() {
	fmt.Println("Starting test_example")
	rescueStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	
	// Example for what gets captured
	fmt.Println(`{"id":357216,"market":4616,"price":40.249484032365004,"volume":688.9720786684491,"is_buy":false}`)

	// Stdout clean up
	w.Close()
	os.Stdout = rescueStdout
	out, _ := ioutil.ReadAll(r)
	var trade Trade	
	json.Unmarshal([]byte(out), &trade)

	fmt.Println("Trade captured")
	fmt.Println(trade)
	fmt.Println(trade.ID)
	fmt.Println(trade.Market)
	fmt.Println(trade.Price)
	fmt.Println(trade.Volume)
	fmt.Println(trade.IsBuy)
}

func main() {
	fmt.Println("Starting program")

	// Create stdout reader and check
	cmd := exec.Command("./data/stdoutinator")
	cmdReader, err := cmd.StdoutPipe()
    if err != nil {
        log.Fatal(err)
    }

	markets := make(map[int]map[string]float64)

	var is_data bool

	// print
    scanner := bufio.NewScanner(cmdReader)
	go func() {
        for scanner.Scan() {
			// fmt.Println("What the scanner retrieved: " + scanner.Text())
			if scanner.Text() == "BEGIN" {
				is_data = true
			} else if scanner.Text() == "END" {
				is_data = false
			} else {
				if is_data == true {
					var trade Trade	
					json.Unmarshal([]byte(scanner.Text()), &trade)

					// Ingest and calculate
					if _, ok := markets[trade.Market]; ok {
						markets[trade.Market]["total_volume"] += trade.Volume
						markets[trade.Market]["total_price"] += trade.Price
						markets[trade.Market]["num_trades"] += 1
						markets[trade.Market]["mean_price"] = (markets[trade.Market]["total_price"] / markets[trade.Market]["num_trades"])
						markets[trade.Market]["mean_volume"] = (markets[trade.Market]["total_volume"] / markets[trade.Market]["num_trades"])
						if trade.IsBuy == true {
							markets[trade.Market]["num_buys"] += 1
							markets[trade.Market]["percent_buys"] = markets[trade.Market]["num_buys"] / markets[trade.Market]["num_trades"]
						}
						markets[trade.Market]["volume_weighted"] = (markets[trade.Market]["mean_price"] * markets[trade.Market]["mean_volume"]) / markets[trade.Market]["total_volume"]
					} else {
						markets[trade.Market] = map[string]float64{}
						markets[trade.Market]["total_volume"] = trade.Volume
						markets[trade.Market]["total_price"] = trade.Price
						markets[trade.Market]["num_trades"] += 1
						markets[trade.Market]["mean_price"] = (markets[trade.Market]["total_price"] / markets[trade.Market]["num_trades"])
						markets[trade.Market]["mean_volume"] = (markets[trade.Market]["total_volume"] / markets[trade.Market]["num_trades"])
						if trade.IsBuy == true {
							markets[trade.Market]["num_buys"] += 1
							markets[trade.Market]["percent_buys"] = markets[trade.Market]["num_buys"] / markets[trade.Market]["num_trades"]
						}
						markets[trade.Market]["volume_weighted"] = (markets[trade.Market]["mean_price"] * markets[trade.Market]["mean_volume"]) / markets[trade.Market]["total_volume"]
					}					
				}
			}
        }
    }()
    if err := cmd.Start(); err != nil {
        log.Fatal(err)
    }
    if err := cmd.Wait(); err != nil {
        log.Fatal(err)
    }

	fmt.Println("End of scanner")
	// fmt.Println(markets) // Works but we need better formatting

	// TODO: Package markets in to json format for output
	for id, market := range markets {
		market["market"] = float64(id)
		market_json, err := json.Marshal(market)
		if err != nil {
			fmt.Printf("Error creating json: %s", err.Error())
		} else {
			fmt.Println(string(market_json))
		}
	}
}