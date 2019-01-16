import zlib
from datetime import datetime
from hyperquant.api import Platform, Sorting, Direction, Interval
from hyperquant.clients import WSClient, Trade, Error, ErrorCode, Endpoint, \
    ParamName, WSConverter, RESTConverter, PlatformRESTClient, PrivatePlatformRESTClient, ItemObject, Candle


class OkexRestConverter(RESTConverter):
    base_url = "https://www.okex.com/api/v1/"  # url to api requests


    endpoint_lookup = {
        Endpoint.TRADE_HISTORY: "trades.do",
        Endpoint.TRADE: "trades.do",
        Endpoint.CANDLE: "kline.do",
    }
    param_value_lookup = {
        Interval.MIN_1: "1min",
        Interval.MIN_3: "3min",
        Interval.MIN_5: "5min",
        Interval.MIN_15: "15min",
        Interval.MIN_30: "30min",
        Interval.HRS_1: "1hour",
        Interval.HRS_2: "2hour",
        Interval.HRS_4: "4hour",
        Interval.HRS_6: "6hour",
        Interval.HRS_8: None,
        Interval.HRS_12: "12hour",
        Interval.DAY_1: "1day",
        Interval.DAY_3: None,
        Interval.WEEK_1: "1week",
        Interval.MONTH_1: None,
    }
    param_name_lookup = {

        ParamName.FROM_TIME: "since",
        ParamName.LIMIT: "size",
        ParamName.INTERVAL: "type",

    }

    param_lookup_by_class = {
        Error: {
            "message": "code",

        },
        Trade: {
            "date": ParamName.TIMESTAMP,
            # "date_ms": ""
            "tid": ParamName.ITEM_ID,
            "price": ParamName.PRICE,
            "amount": ParamName.AMOUNT,
            "type": ParamName.DIRECTION,
        },
        Candle: [
            ParamName.TIMESTAMP,
            ParamName.PRICE_OPEN,
            ParamName.PRICE_HIGH,
            ParamName.PRICE_LOW,
            ParamName.PRICE_CLOSE,
            ParamName.AMOUNT
        ]

    }

    def _convert_timestamp_from_platform(self, timestamp):
        return super()._convert_timestamp_from_platform(int(timestamp))

    def parse(self, endpoint, data):
        if endpoint == Endpoint.CANDLE:
            self.is_source_in_milliseconds = True
        else:
            self.is_source_in_milliseconds = False
        result = super().parse(endpoint, data)

        return result




class OkexRestClient(PrivatePlatformRESTClient):
    default_converter_class = OkexRestConverter
    platform_id = Platform.OKEX
    version = "1"

    @property
    def headers(self):
        result = super().headers
        result["Content-Type"] = "application/x-www-form-urlencoded"
        return result

    def fetch_trades_history(self, symbol=None, limit=None, from_item=None,
                             sorting=None, from_time=None, to_time=None, **kwargs):
        return super().fetch_trades_history(symbol, limit, from_item, sorting=sorting,
                                            from_time=from_time, to_time=to_time, **kwargs)

    def fetch_candles(self, symbol, interval, limit=None, from_time=None, to_time=None,
                      is_use_max_limit=False, version=None, **kwargs):

        a = super().fetch_candles(symbol, interval, limit, from_time, to_time, is_use_max_limit, version, **kwargs)
        return a


class OkexWSConverter(WSConverter):
    is_source_in_milliseconds = True

    base_url = "wss://real.okex.com:10440/ws/v1/"  # url to api requests
    event_type_param = "channel"

    endpoint_lookup = {
        Endpoint.TRADE: "ok_sub_spot_{symbol}_deals",
        Endpoint.CANDLE: "ok_sub_spot_{symbol}_kline_{interval}",

    }
    param_value_lookup = {
        Interval.MIN_1: "1min",
        Interval.MIN_3: "3min",
        Interval.MIN_5: "5min",
        Interval.MIN_15: "15min",
        Interval.MIN_30: "30min",
        Interval.HRS_1: "1hour",
        Interval.HRS_2: "2hour",
        Interval.HRS_4: "4hour",
        Interval.HRS_6: "6hour",
        Interval.HRS_8: None,
        Interval.HRS_12: "12hour",
        Interval.DAY_1: "1day",
        Interval.DAY_3: None,
        Interval.WEEK_1: "1week",
        Interval.MONTH_1: None,
    }

    param_lookup_by_class = {
        Error: {
            "error_code": "code",
            "error_msg": "message",
        },

        Trade: [
            ParamName.ITEM_ID,
            ParamName.PRICE,
            ParamName.AMOUNT,
            ParamName.TIMESTAMP,
            ParamName.ORDER_TYPE

        ],
        Candle: [
            ParamName.TIMESTAMP,

            ParamName.PRICE_OPEN,
            ParamName.PRICE_HIGH,
            ParamName.PRICE_LOW,
            ParamName.PRICE_CLOSE,
            ParamName.AMOUNT
        ],

    }

    #  supported_symbols = ["ltc_btc"]

    def parse(self, endpoint, data):
        if data:

            endpoint = data.get(self.event_type_param)
            if endpoint == "addChannel":
                return
            if "data" in data:
                data = data["data"]
        return super().parse(endpoint, data)

    def post_process_result(self, endpoint, platform_endpoint, item):
        symbol_pos = self.endpoint_lookup.get(endpoint).find("{symbol}")
        symbol = platform_endpoint[symbol_pos:(len(platform_endpoint)-symbol_pos)+1]
        interval_pos = self.endpoint_lookup.get(endpoint).find("{interval}")
        interval = platform_endpoint[interval_pos:]
        self._propagate_param_to_result(ParamName.SYMBOL, symbol, item)
        self._propagate_param_to_result(ParamName.INTERVAL, interval, item)
        return item

    def string_time_to_timestamp(self, item_data, param_class, to_millisecods = False):
        time = datetime.utcnow()
        time_diff = (time.minute*60) + time.second
        time = time.timestamp() - time_diff
        lookup = self.param_lookup_by_class.get(param_class)
        time_pos = lookup.index(ParamName.TIMESTAMP)
        platform_time = item_data[time_pos]
        platform_time = platform_time[platform_time.find(":")+1:]

        platform_time = platform_time.split(":")
        time_diff = (int(platform_time[0])*60)+int(platform_time[1])
        time += time_diff
        if to_millisecods:
            time *= 1000
        item_data[time_pos] = time




    def _propagate_param_to_result(self, param_name, param, item):
        if isinstance(item, list):

            for items in item:
                if hasattr(items, param_name):
                    setattr(item, param_name, param)
        else:
            if hasattr(item, param_name):
                setattr(item, param_name, param)
        return item

    def _parse_item(self, endpoint, item_data):

        if endpoint.find("kline") != -1:


            item = super()._parse_item(Endpoint.CANDLE, item_data)

            self.post_process_result(Endpoint.CANDLE, endpoint, item)
        elif endpoint.find("deals") != -1:
            self.string_time_to_timestamp(item_data, Trade, True)

            item = super()._parse_item(Endpoint.TRADE, item_data)
            self.post_process_result(Endpoint.TRADE, endpoint, item)
        return item

    def _convert_timestamp_from_platform(self, timestamp):
        if self.is_source_in_milliseconds:
            timestamp = int(timestamp) / 1000
        return timestamp

    def _generate_subscription(self, endpoint, symbol=None, **params):
        for key, item in params.items():
            params.update({key: super()._get_platform_param_value(item, None)})
        return super()._generate_subscription(endpoint, symbol, **params)


class OkexWSClient(WSClient):
    params_buff = None
    platform_id = Platform.OKEX
    version = "1"
    default_converter_class = OkexWSConverter

    def subscribe(self, endpoints=None, symbols=None, **params):
        if params:
            self.params_buff = params
        super().subscribe(endpoints=endpoints, symbols=symbols, **self.params_buff)

    @staticmethod
    def _decompress(message):
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        data = decompress.decompress(message)
        data += decompress.flush()
        return data

    def _on_message(self, message):

        super()._on_message(self._decompress(message))

    def _send_subscribe(self, subscriptions):
        message = []

        for channels in subscriptions:
            message.append({"event": "addChannel", "channel": channels})
        self._send(message)

    def on_item_received(self, item):
        if isinstance(item, list):
            for items in item:
                item = items
        super().on_item_received(item)



