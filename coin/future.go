package trader

import (
	"context"
	"errors"
	"fmt"
	"github.com/adshao/go-binance/v2"
	"github.com/adshao/go-binance/v2/futures"
	"log"
	"math"
	"strconv"
	"time"
)

const ContractTradingStatus = "TRADING"
const ContractSymbolFilterType = "LOT_SIZE"
const ContractPriceFilterType = "PRICE_FILTER"
const ContractMinNotionalFilterType = "MIN_NOTIONAL"

const OrderImmediatelyTrig = -2021

type ErrorResponse struct {
	Code    int64  `json:"code"`
	Message string `json:"msg"`
}

type OrderRequest struct {
	symbol           string
	side             futures.SideType
	positionSide     futures.PositionSideType
	orderType        futures.OrderType
	quantity         string
	newOrderRespType futures.NewOrderRespType
	timeInForce      futures.TimeInForceType
	//reduceOnly       bool
	price            string
	stopPrice        string
	newClientOrderID string

	//workingType      *WorkingType
	//activationPrice  *string
	//callbackRate     *string
	//priceProtect     *bool
	//closePosition    *bool
}

type FutureClient struct {
	apiKey    string
	secretKey string
	future    *futures.Client
}

type PriceFilter struct {
	MaxPrice float64 `json:"maxPrice"`
	MinPrice float64 `json:"minPrice"`
	TickSize float64 `json:"tickSize"`
}

type LotSize struct {
	//filterType string  `json:"filterType"`
	MaxQty   float64 `json:"maxQty"`
	MinQty   float64 `json:"minQty"`
	StepSize float64 `json:"stepSize"`
}

type SymbolPair struct {
	Symbol       string               `json:"symbol"`
	Pair         string               `json:"pair"`
	ContractType futures.ContractType `json:"contractType"`
	DeliveryDate int64                `json:"deliveryDate"`
	Status       string               `json:"status"`
	BaseAsset    string               `json:"baseAsset"`
	QuoteAsset   string               `json:"quoteAsset"`
	MinNotional  float64              `json:"minNotional"`
	LotSize
	PriceFilter
}

type SymbolPairRequest struct {
	Symbol string `json:"symbol"`
}

type SymbolRequest struct {
	Name   string `json:"name"`
	Symbol string `json:"symbol"`
}

type SymbolResponse struct {
	Symbol     string `json:"symbol"`
	QuoteAsset string `json:"quoteAsset"`
	LotSize
	AvailableBalance float64 `json:"availableBalance"`
	Price            float64 `json:"price"`
	Quantity         float64 `json:"quantity"`
}

type PriceResponse struct {
	Symbol string  `json:"symbol"`
	Price  float64 `json:"price"`
	Time   string  `json:"time"`
}

func NewFutureClient(apiKey string, secretKey string) *FutureClient {
	futuresClient := binance.NewFuturesClient(apiKey, secretKey) // USDT-M Futures
	futuresClient.NewSetServerTimeService().Do(context.Background())

	client := &FutureClient{
		apiKey:    apiKey,
		secretKey: secretKey,
		future:    futuresClient,
	}
	go func() {
		for {
			// 每隔20min setTime
			client.SetServerTime()
			time.Sleep(20 * time.Minute)
		}
	}()
	return client
}

func (f *FutureClient) Ping() bool {
	err := f.future.NewPingService().Do(context.Background())
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (f *FutureClient) Pair() ([]SymbolPair, error) {
	exchange, err := f.future.NewExchangeInfoService().Do(context.Background())
	if err != nil {
		log.Println(err)
		return nil, err
	}
	var pairArray []SymbolPair
	for _, item := range exchange.Symbols {
		if item.Status == ContractTradingStatus && item.ContractType == futures.ContractTypePerpetual {
			symbolPair := SymbolPair{
				Symbol:       item.Symbol,
				Pair:         item.Pair,
				ContractType: item.ContractType,
				DeliveryDate: item.DeliveryDate,
				Status:       item.Status,
				BaseAsset:    item.BaseAsset,
				QuoteAsset:   item.QuoteAsset,
			}
			for _, filter := range item.Filters {
				if filter["filterType"] == ContractSymbolFilterType {
					//jsonString, _ := json.Marshal(filter)
					//json.Unmarshal(jsonString, &symbolPair.LotSize)
					symbolPair.MaxQty, _ = strconv.ParseFloat(filter["maxQty"].(string), 64)
					symbolPair.StepSize, _ = strconv.ParseFloat(filter["stepSize"].(string), 64)
					symbolPair.MinQty, _ = strconv.ParseFloat(filter["minQty"].(string), 64)
				}
				if filter["filterType"] == ContractPriceFilterType {
					symbolPair.TickSize, _ = strconv.ParseFloat(filter["tickSize"].(string), 64)
					symbolPair.MaxPrice, _ = strconv.ParseFloat(filter["maxPrice"].(string), 64)
					symbolPair.MinPrice, _ = strconv.ParseFloat(filter["minPrice"].(string), 64)
				}
				if filter["filterType"] == ContractMinNotionalFilterType {
					symbolPair.MinNotional, _ = strconv.ParseFloat(filter["notional"].(string), 64)
				}
			}
			pairArray = append(pairArray, symbolPair)
		}
	}
	return pairArray, nil
}

func (f *FutureClient) PairBySymbol(symbol string) (*SymbolPair, error) {
	pairArray, err := f.Pair()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	for _, pair := range pairArray {
		if pair.Symbol == symbol {
			return &pair, nil
		}
	}
	return nil, errors.New("交易对不存在")
}

func (f *FutureClient) ChangeLeverage(symbol string, leverage int) error {
	_, err := f.future.NewChangeLeverageService().Symbol(symbol).Leverage(leverage).Do(context.Background())
	return err
}

func (f *FutureClient) ChangeMargin(symbol string, maginType futures.MarginType) error {
	return f.future.NewChangeMarginTypeService().Symbol(symbol).MarginType(maginType).Do(context.Background())
}

func (f *FutureClient) MaxLeverage(symbol string) (int, error) {
	result, err := f.future.NewGetLeverageBracketService().Symbol(symbol).Do(context.Background())
	if err != nil {
		log.Println(err)
		return 0, err
	}
	if len(result) <= 0 {
		log.Println("error not found symbol " + symbol)
		return 0, errors.New(symbol + " 交易对不存在")
	}
	return result[0].Brackets[0].InitialLeverage, nil
}

func (f *FutureClient) GetPosition(symbol string, side futures.PositionSideType) (*futures.AccountPosition, error) {
	account, err := f.Account()
	if err != nil {
		return nil, err
	}
	if account == nil {
		log.Println("account data error")
	}
	for _, position := range account.Positions {
		if position.Symbol == symbol && position.PositionSide == side {
			positionAmt, _ := strconv.ParseFloat(position.PositionAmt, 64)
			if math.Abs(positionAmt) > 0 {
				return position, nil
			}
		}
	}
	return nil, nil
}

func (f *FutureClient) AllPosition() ([]*futures.AccountPosition, error) {
	account, err := f.Account()
	if err != nil {
		return nil, err
	}
	if account == nil {
		log.Println("account data error")
	}
	var result []*futures.AccountPosition
	for _, position := range account.Positions {
		if position != nil {
			positionAmt, _ := strconv.ParseFloat(position.PositionAmt, 64)
			if math.Abs(positionAmt) > 0 {
				result = append(result, position)
			}
		}
	}
	return result, nil
}

func (f *FutureClient) PositionRisk(symbol string) (string, int, error) {
	data, err := f.future.NewGetPositionRiskService().Symbol(symbol).Do(context.Background())
	if err != nil {
		return "", 0, err
	}
	leverage, err := strconv.Atoi(data[0].Leverage)
	return data[0].MarginType, leverage, nil
}

func (f *FutureClient) Account() (*futures.Account, error) {
	accountData, err := f.future.NewGetAccountService().Do(context.Background())
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return accountData, nil
}

func (f *FutureClient) Asserts() ([]*futures.AccountAsset, error) {
	account, err := f.Account()
	if err != nil {
		return nil, err
	}
	var result []*futures.AccountAsset
	for _, assert := range account.Assets {
		walletBalance, _ := strconv.ParseFloat(assert.WalletBalance, 64)
		if walletBalance > 0 {
			result = append(result, assert)
		}
	}
	return result, nil
}

func (f *FutureClient) AssertsByQuote(quoteSymbol string) (float64, error) {
	asserts, err := f.Asserts()
	if err != nil {
		return 0, nil
	}
	if asserts == nil {
		return 0, nil
	}
	for _, assert := range asserts {
		if assert.Asset == quoteSymbol {
			value, err := strconv.ParseFloat(assert.MaxWithdrawAmount, 64)
			return value, err
		}
	}
	return 0, nil
}

func (f *FutureClient) TestPrice(symbol string) (res *futures.SymbolPrice, err error) {
	symbolPrice, err := f.future.NewListPricesService().Symbol(symbol).Do(context.Background())
	return symbolPrice[0], err
}

func (f *FutureClient) TestWssBookTicker(symbol string) {
	WsBookTickerHandler := func(event *futures.WsBookTickerEvent) {
		fmt.Println(event)
	}
	doneC, stopC, err := futures.WsBookTickerServe(symbol, WsBookTickerHandler, func(err error) {
		log.Println(err)
	})
	if err != nil {
		log.Println(err)
	}
	// use stopC to exit
	go func() {
		time.Sleep(20 * time.Second)
		stopC <- struct{}{}
	}()
	// remove this if you do not want to be blocked here
	<-doneC
}

func (f *FutureClient) TestAllCombinedBookTicker(symbol []string) {
	WsBookTickerHandler := func(event *futures.WsBookTickerEvent) {
		fmt.Println(event)
	}
	doneC, stopC, err := futures.WsAllBookTickerServe(WsBookTickerHandler, func(err error) {
		log.Println("错误: ", err)
	})
	if err != nil {
		log.Println(err)
	}
	// use stopC to exit
	go func() {
		time.Sleep(20 * time.Hour)
		stopC <- struct{}{}
	}()
	// remove this if you do not want to be blocked here
	<-doneC
}

func (f *FutureClient) TestCombinedPrice(symbol []string) {
	//var priceList []*futures.WsAggTradeEvent
	userHandler := func(event *futures.WsAggTradeEvent) {
		log.Println(event)
		//priceList = append(priceList, event)
	}
	doneC, stopC, err := futures.WsCombinedAggTradeServe(symbol, userHandler, func(err error) {
		log.Println(err)
	})
	log.Println("start aggTrade")
	if err != nil {
		log.Println(err)
	}
	// use stopC to exit
	go func() {
		time.Sleep(20 * time.Second)
		stopC <- struct{}{}
	}()
	// remove this if you do not want to be blocked here
	<-doneC
	//return priceList
}

func (f *FutureClient) TestWssPrice(symbol string) []*futures.WsAggTradeEvent {
	var priceList []*futures.WsAggTradeEvent
	userHandler := func(event *futures.WsAggTradeEvent) {
		//log.Println(event)
		priceList = append(priceList, event)
	}
	doneC, stopC, err := futures.WsAggTradeServe(symbol, userHandler, func(err error) {
		log.Println(err)
	})
	log.Println("start")
	if err != nil {
		log.Println(err)
	}
	// use stopC to exit
	go func() {
		time.Sleep(30 * time.Second)
		stopC <- struct{}{}
	}()
	// remove this if you do not want to be blocked here
	<-doneC
	return priceList
}

func (f *FutureClient) TestAllPrice(symbol string) (res *futures.SymbolPrice, err error) {
	symbolPrice, err := f.future.NewListPricesService().Do(context.Background())
	for _, item := range symbolPrice {
		if item.Symbol == symbol {
			return item, nil
		}
	}
	return symbolPrice[0], err
}

func (f *FutureClient) TicketAllPrice(symbol string) (float64, error) {
	symbolPrice, err := f.future.NewListPricesService().Do(context.Background())
	if err != nil {
		log.Println(err)
		return 0, err
	}
	for _, item := range symbolPrice {
		if item.Symbol == symbol {
			price, err := strconv.ParseFloat(item.Price, 64)
			if err != nil {
				log.Println(err)
				return 0, err
			}
			return price, nil
		}
	}
	return 0, errors.New("交易对不存在")
}

func (f *FutureClient) BookTickerPrice() (res []*futures.BookTicker, err error) {
	resp, err := f.future.NewListBookTickersService().Do(context.Background())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (f *FutureClient) TickerPrice(symbol string) (float64, error) {
	symbolPrice, err := f.future.NewListPricesService().Symbol(symbol).Do(context.Background())
	if err != nil {
		log.Println(err)
		return 0, err
	}
	price, err := strconv.ParseFloat(symbolPrice[0].Price, 64)
	if err != nil {
		log.Println(err)
		return 0, err
	}
	return price, nil
}

func (f *FutureClient) MarkerPrice() {

}

func (f *FutureClient) Balance() (*futures.Balance, error) {
	balance, err := f.future.NewGetBalanceService().Do(context.Background())
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return balance[0], err
}

func (f *FutureClient) Depth(symbol string) (*futures.DepthResponse, error) {
	return f.future.NewDepthService().Symbol(symbol).Limit(5).Do(context.Background())
}

func (f *FutureClient) PlaceLimitOrder(order OrderRequest) (*futures.CreateOrderResponse, error) {
	// 下单
	orderService := f.future.NewCreateOrderService().Symbol(order.symbol).Side(order.side).PositionSide(order.positionSide).Quantity(order.quantity).Type(order.orderType)
	orderService.NewClientOrderID(fmt.Sprintf("%v%v%v", order.symbol, time.Now().UnixNano(), "M"))
	orderResponse, err := orderService.Price(order.price).TimeInForce(order.timeInForce).NewOrderResponseType(order.newOrderRespType).Do(context.Background())
	return orderResponse, err
}

func (f *FutureClient) PlaceStopOrder(order OrderRequest) (*futures.CreateOrderResponse, error) {
	// 下单
	orderService := f.future.NewCreateOrderService().Symbol(order.symbol).Side(order.side).PositionSide(order.positionSide).Quantity(order.quantity).Type(order.orderType)
	if order.orderType == futures.OrderTypeTakeProfit {
		orderService.NewClientOrderID(fmt.Sprintf("%v%v%v", order.symbol, time.Now().UnixNano(), "P"))
	} else {
		orderService.NewClientOrderID(fmt.Sprintf("%v%v%v", order.symbol, time.Now().UnixNano(), "L"))
	}
	orderResponse, err := orderService.Price(order.price).StopPrice(order.stopPrice).NewOrderResponseType(order.newOrderRespType).Do(context.Background())
	return orderResponse, err
}

func (f *FutureClient) PlaceStopMarketOrder(order OrderRequest) (*futures.CreateOrderResponse, error) {
	// 下单
	orderService := f.future.NewCreateOrderService().Symbol(order.symbol).Side(order.side).PositionSide(order.positionSide).Quantity(order.quantity).Type(order.orderType)
	if order.orderType == futures.OrderTypeTakeProfitMarket {
		orderService.NewClientOrderID(fmt.Sprintf("%v%v%v", order.symbol, time.Now().UnixNano(), "P"))
	} else {
		orderService.NewClientOrderID(fmt.Sprintf("%v%v%v", order.symbol, time.Now().UnixNano(), "L"))
	}
	orderResponse, err := orderService.StopPrice(order.stopPrice).NewOrderResponseType(order.newOrderRespType).Do(context.Background())
	return orderResponse, err
}

func (f *FutureClient) PlaceMarketOrder(order OrderRequest) (*futures.CreateOrderResponse, error) {
	// 下单
	orderService := f.future.NewCreateOrderService().Symbol(order.symbol).Side(order.side).PositionSide(order.positionSide).Quantity(order.quantity).Type(order.orderType)
	orderService.NewClientOrderID(fmt.Sprintf("%v%v", order.symbol, time.Now().UnixNano()))
	orderResponse, err := orderService.NewOrderResponseType(order.newOrderRespType).Do(context.Background())
	return orderResponse, err
}

func (f *FutureClient) GetAllOpenOrder(symbol string) ([]*futures.Order, error) {
	return f.future.NewListOpenOrdersService().Symbol(symbol).Do(context.Background())
}

func (f *FutureClient) GetCurrentOpenOrder(symbol string, orderId int64) (*futures.Order, error) {
	return f.future.NewGetOrderService().Symbol(symbol).OrderID(orderId).Do(context.Background())
}

func (f *FutureClient) GetOpenOrder(symbol string, orderId int64) (*futures.Order, error) {
	return f.future.NewGetOrderService().Symbol(symbol).OrderID(orderId).Do(context.Background())
}

func (f *FutureClient) CancelOrder(symbol string, orderId int64) (res *futures.CancelOrderResponse, err error) {
	return f.future.NewCancelOrderService().Symbol(symbol).OrderID(orderId).Do(context.Background())
}

func (f *FutureClient) CancelAllOpenOrder(symbol string) error {
	return f.future.NewCancelAllOpenOrdersService().Symbol(symbol).Do(context.Background())
}

func (f *FutureClient) UserTrades(symbol string, startTime int64, endTime int64) (res []*futures.AccountTrade, err error) {
	request := f.future.NewListAccountTradeService().Symbol(symbol)
	if startTime > 0 {
		request = request.StartTime(startTime)
	}
	if endTime > 0 {
		request = request.EndTime(endTime)
	}
	return request.Do(context.Background())
}

func (f *FutureClient) UserTradesByOrderId(orderId int64, symbol string, startTime int64, endTime int64) (*futures.AccountTrade, error) {
	res, err := f.UserTrades(symbol, startTime, endTime)
	if err != nil {
		return nil, err
	}
	for _, item := range res {
		if item.OrderID == orderId {
			return item, nil
		}
	}
	return nil, errors.New("交易对没有成交记录")
}

func (f *FutureClient) SportTickerPrice(symbol string) (*binance.SymbolPrice, error) {
	client := NewSportClient("", "")
	return client.TickerPrice(symbol)
}

func (f *FutureClient) SetServerTime() (timeOffset int64, err error) {
	return f.future.NewSetServerTimeService().Do(context.Background())
}

func (f *FutureClient) NewListenKey() (listenKey string, err error) {
	return f.future.NewStartUserStreamService().Do(context.Background())
}

func (f *FutureClient) DelayListenKey(listenKey string) error {
	return f.future.NewKeepaliveUserStreamService().ListenKey(listenKey).Do(context.Background())
}

func (f *FutureClient) CloseListenKey(listenKey string) error {
	return f.future.NewCloseUserStreamService().ListenKey(listenKey).Do(context.Background())
}
