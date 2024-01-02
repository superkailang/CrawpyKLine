package trader

import (
	"context"
	"github.com/adshao/go-binance/v2"
	"log"
)

type SportClient struct {
	apiKey    string
	secretKey string
	sport     *binance.Client
}

func NewSportClient(apiKey string, secretKey string) *SportClient {
	sportClient := binance.NewClient(apiKey, secretKey)
	return &SportClient{
		apiKey:    apiKey,
		secretKey: secretKey,
		sport:     sportClient,
	}
}

func (f *SportClient) Ping() bool {
	err := f.sport.NewPingService().Do(context.Background())
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (f *SportClient) Transfer(_type string, _asset string, _amount float64) error {
	_, err := f.sport.NewUserUniversalTransferService().Type(_type).Asset(_asset).Amount(_amount).Do(context.Background())
	return err
}

func (f *SportClient) TransferSportToFuture() error {
	// 现货 => 合约
	//f.Transfer(MAIN_UMFUTURE, self.quote_asset, _amount)
	return nil
}

func (f *SportClient) TransferFutureToSport() error {
	// U本位 => 现货
	//f.Transfer(UMFUTURE_MAIN, self.quote_asset, _amount)
	return nil
}

func (f *SportClient) TickerPrice(symbol string) (*binance.SymbolPrice, error) {
	res, err := f.sport.NewListPricesService().Symbol(symbol).Do(context.Background())
	if err != nil {
		return nil, err
	}
	return res[0], nil
}

func (f *SportClient) KLinePrice(symbol string, ) {
	f.sport.NewKlinesService().Symbol(symbol)
}
