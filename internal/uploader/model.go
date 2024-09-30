package uploader

import (
	"fmt"

	"github.com/goroumaru/sortedmap"
)

type datum struct {
	Date      string
	ProjectID string
	EventType string
	VolumeUSD float64

	lineNumber int
	err        error
}

type DataMapDatum struct {
	VolumeUSD           float64
	NumberOfTransaction int64
}

type DataMap map[string]map[string]map[string]*DataMapDatum

func (d *DataMap) Insert(data *datum) {
	dayDataMap := (*d)[data.Date]
	if dayDataMap == nil {
		dayDataMap = make(map[string]map[string]*DataMapDatum)
		(*d)[data.Date] = dayDataMap
	}
	projectDataMap := dayDataMap[data.ProjectID]
	if projectDataMap == nil {
		projectDataMap = make(map[string]*DataMapDatum)
		dayDataMap[data.ProjectID] = projectDataMap
	}
	dataMapDatum := projectDataMap[data.EventType]
	if dataMapDatum == nil {
		dataMapDatum = &DataMapDatum{}
		projectDataMap[data.EventType] = dataMapDatum
	}
	dataMapDatum.VolumeUSD += data.VolumeUSD
	dataMapDatum.NumberOfTransaction++
}

func (d *DataMap) Print() {
	for _, dayElem := range sortedmap.AsSortedMap(*d) {
		fmt.Println(dayElem.Key)
		for _, projectElem := range sortedmap.AsSortedMap(dayElem.Value) {
			fmt.Println("	", projectElem.Key)
			for _, eventElem := range sortedmap.AsSortedMap(projectElem.Value) {
				fmt.Println("		", eventElem.Key, *eventElem.Value)
			}
		}
	}
}

type props struct {
	CurrencySymbol  string `json:"currencySymbol"`
	CurrencyAddress string `json:"currencyAddress"`
	ChainID         string `json:"chainId"`
	MarketplaceType string `json:"marketplaceType"`
}

type nums struct {
	CurrencyValueDecimal string `json:"currencyValueDecimal"`
}
