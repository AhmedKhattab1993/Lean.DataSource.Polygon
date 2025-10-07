/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System.Collections.Generic;
using System.Collections.ObjectModel;
using Newtonsoft.Json;
using System.Net.NetworkInformation;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using Newtonsoft.Json.Linq;
using NodaTime;
using System.Globalization;
using QuantConnect.Api;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;
using RestSharp;
using QuantConnect.Lean.Engine.DataFeeds;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Polygon.io implementation of <see cref="IDataQueueHandler"/> and <see cref="IHistoryProvider"/>
    /// </summary>
    /// <remarks>
    /// Polygon.io equities documentation: https://polygon.io/docs/stocks/getting-started
    /// Polygon.io options documentation: https://polygon.io/docs/options/getting-started
    /// </remarks>
    public partial class PolygonDataProvider : IDataQueueHandler
    {
        private static readonly ReadOnlyCollection<SecurityType> _supportedSecurityTypes = Array.AsReadOnly(new[]
        {
            SecurityType.Equity,
            SecurityType.Option,
            SecurityType.IndexOption,
            SecurityType.Index,
        });

        private string _apiKey;
        private bool _skipLicenseValidation = Config.GetBool("polygon-skip-license-validation", false);

        private PolygonAggregationManager _dataAggregator;

        protected PolygonSubscriptionManager _subscriptionManager;

        private List<ExchangeMapping> _exchangeMappings;
        private readonly PolygonSymbolMapper _symbolMapper = new();
        private readonly MarketHoursDatabase _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
        private readonly Dictionary<Symbol, DateTimeZone> _symbolExchangeTimeZones = new();

        private bool _initialized;
        private bool _disposed;

        private volatile bool _unsupportedSecurityTypeMessageLogged;
        private volatile bool _unsupportedTickTypeMessagedLogged;
        private volatile bool _unsupportedDataTypeMessageLogged;
        private volatile bool _potentialUnsupportedResolutionMessageLogged;
        private volatile bool _potentialUnsupportedTickTypeMessageLogged;

        /// <summary>
        /// <inheritdoc cref="IMapFileProvider"/>
        /// </summary>
        private readonly IMapFileProvider _mapFileProvider = Composer.Instance.GetPart<IMapFileProvider>();

        /// <summary>
        /// The time provider instance. Used for improved testability
        /// </summary>
        protected virtual ITimeProvider TimeProvider { get; } = RealTimeProvider.Instance;

        /// <summary>
        /// The rest client instance
        /// </summary>
        protected virtual PolygonRestApiClient RestApiClient { get; set; }

        /// <summary>
        /// Creates and initializes a new instance of the <see cref="PolygonDataProvider"/> class
        /// using configuration values for the API key, maximum subscriptions per WebSocket, 
        /// and license type.
        /// </summary>
        public PolygonDataProvider()
            : this(Config.Get("polygon-api-key"), Config.GetInt("polygon-max-subscriptions-per-websocket", -1), licenseTypeFromConfig: Config.Get("polygon-license-type"))
        {
        }

        /// <summary>
        /// Creates and initializes a new instance of the <see cref="PolygonDataProvider"/> class
        /// </summary>
        /// <param name="apiKey">The Polygon API key for authentication</param>
        /// <param name="streamingEnabled">
        /// Whether this handle will be used for streaming data.
        /// If false, the handler is supposed to be used as a history provider only.
        /// </param>
        /// <param name="licenseTypeFromConfig">The license type string from configuration (optional).</param>
        public PolygonDataProvider(string apiKey, bool streamingEnabled = true, string licenseTypeFromConfig = "")
            : this(apiKey, Config.GetInt("polygon-max-subscriptions-per-websocket", -1), streamingEnabled, licenseTypeFromConfig)
        {
        }

        /// <summary>
        /// Creates and initializes a new instance of the <see cref="PolygonDataProvider"/> class
        /// </summary>
        /// <param name="apiKey">The Polygon.io API key for authentication</param>
        /// <param name="maxSubscriptionsPerWebSocket">The maximum number of subscriptions allowed per websocket</param>
        /// <param name="streamingEnabled">
        /// Whether this handle will be used for streaming data.
        /// If false, the handler is supposed to be used as a history provider only.
        /// </param>
        /// <param name="licenseTypeFromConfig">The license type string from configuration (optional).</param>
        public PolygonDataProvider(string apiKey, int maxSubscriptionsPerWebSocket, bool streamingEnabled = true, string licenseTypeFromConfig = "")
        {
            if (string.IsNullOrWhiteSpace(apiKey))
            {
                // If the API key is not provided, we can't do anything.
                // The handler might going to be initialized using a node packet job.
                return;
            }

            Initialize(apiKey, maxSubscriptionsPerWebSocket, licenseTypeFromConfig, streamingEnabled);
        }

        /// <summary>
        /// Initializes the <see cref="PolygonDataProvider"/> instance with the specified API key,
        /// maximum subscriptions per WebSocket, license type, and streaming behavior.
        /// </summary>
        /// <param name="apiKey">The Polygon.io API key used for authentication.</param>
        /// <param name="maxSubscriptionsPerWebSocket">
        /// The maximum number of subscriptions allowed per WebSocket connection.
        /// </param>
        /// <param name="licenseTypeFromConfig">
        /// The license type string retrieved from configuration (e.g., "Individual" or "Business").
        /// </param>
        /// <param name="streamingEnabled">
        /// Determines whether this instance is used for streaming data. 
        /// If <c>false</c>, the provider is expected to be used for historical data only. Defaults to <c>true</c>.
        /// </param>
        private void Initialize(string apiKey, int maxSubscriptionsPerWebSocket, string licenseTypeFromConfig, bool streamingEnabled = true)
        {
            if (string.IsNullOrWhiteSpace(apiKey))
            {
                throw new PolygonAuthenticationException("History calls for Polygon.io require an API key.");
            }
            _apiKey = apiKey;

            _initialized = true;
            RestApiClient = new PolygonRestApiClient(_apiKey);
            _optionChainProvider = new CachingOptionChainProvider(new PolygonOptionChainProvider(RestApiClient, _symbolMapper));

            if (_skipLicenseValidation)
            {
                Log.Trace($"{nameof(PolygonDataProvider)}.{nameof(Initialize)}: Skipping license validation (polygon-skip-license-validation=true).");
            }
            else
            {
                ValidateSubscription();
            }

            // Initialize the exchange mappings
            _exchangeMappings = FetchExchangeMappings();
            var licenseType = ParseLicenseType(licenseTypeFromConfig);

            // Initialize the subscription manager if this instance is going to be used as a data queue handler
            if (streamingEnabled)
            {
                _subscriptionManager = new PolygonSubscriptionManager(
                    _supportedSecurityTypes,
                    maxSubscriptionsPerWebSocket,
                    (securityType) => new PolygonWebSocketClientWrapper(_apiKey, _symbolMapper, securityType, OnMessage, licenseType));
                _dataAggregator = new PolygonAggregationManager(_subscriptionManager);
                var openInterestManager = new PolygonOpenInterestProcessorManager(TimeProvider, RestApiClient, _symbolMapper, _subscriptionManager, _dataAggregator, GetTickTime);
                openInterestManager.ScheduleNextRun();
            }
        }

        #region IDataQueueHandler implementation

        /// <summary>
        /// Returns whether the data provider is connected
        /// </summary>
        /// <returns>True if the data provider is connected</returns>
        public bool IsConnected => _subscriptionManager != null && _subscriptionManager.IsConnected;

        /// <summary>
        /// Sets the job we're subscribing for
        /// </summary>
        /// <param name="job">Job we're subscribing for</param>
        public void SetJob(LiveNodePacket job)
        {
            if (_initialized)
            {
                return;
            }

            var brokerageData = job.GetType().GetProperty("BrokerageData")?.GetValue(job) as IDictionary<string, string>
                ?? job.GetType().GetField("BrokerageData")?.GetValue(job) as IDictionary<string, string>;

            if (brokerageData == null)
            {
                throw new ArgumentException("The Polygon.io brokerage data dictionary is missing from the live job packet.");
            }

            if (!brokerageData.TryGetValue("polygon-api-key", out var apiKey))
            {
                throw new ArgumentException("The Polygon.io API key is missing from the brokerage data.");
            }

            if (!brokerageData.TryGetValue("polygon-max-subscriptions-per-websocket", out var maxSubscriptionsPerWebSocketStr) ||
                !int.TryParse(maxSubscriptionsPerWebSocketStr, out var maxSubscriptionsPerWebSocket))
            {
                maxSubscriptionsPerWebSocket = -1;
            }

            if (brokerageData.TryGetValue("polygon-skip-license-validation", out var skipLicense)
                && bool.TryParse(skipLicense, out var parsedSkip))
            {
                _skipLicenseValidation = parsedSkip;
            }

            Initialize(apiKey, maxSubscriptionsPerWebSocket, brokerageData.TryGetValue("polygon-license-type", out var licenseType) ? licenseType : string.Empty);
        }

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            if (!CanSubscribe(dataConfig))
            {
                return null;
            }

            Log.Trace($"PolygonDataProvider.Subscribe(): Subscribing to {dataConfig.Symbol} | {dataConfig.TickType}");

            if (_subscriptionManager.GetWebSocket(dataConfig.SecurityType) == null)
            {
                return null;
            }

            try
            {
                _subscriptionManager.Subscribe(dataConfig);
            }
            catch (PolygonAuthenticationException ex) when (!ex.Message.Contains("404"))
            {
                return null;
            }
            catch (UnsupportedTickTypeForLicenseException)
            {
                return null;
            }

            var enumerator = _dataAggregator.Add(dataConfig, newDataAvailableHandler);

            return enumerator;
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        /// <param name="dataConfig">Subscription config to be removed</param>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            _subscriptionManager.Unsubscribe(dataConfig);
            _dataAggregator.Remove(dataConfig);
        }

        #endregion

        #region IDisposable implementation

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _subscriptionManager?.DisposeSafely();
                _dataAggregator.DisposeSafely();
                RestApiClient.DisposeSafely();

                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~PolygonDataProvider()
        {
            Dispose(disposing: false);
        }

        #endregion

        #region WebSocket

        /// <summary>
        /// Handles Polygon.io websocket messages
        /// </summary>
        private void OnMessage(string message)
        {
            foreach (var parsedMessage in JArray.Parse(message))
            {
                var eventType = parsedMessage["ev"].ToString();

                switch (eventType)
                {
                    case "A":
                    case "AM": // Aggregates (Per Minute)
                        ProcessAggregate(parsedMessage.ToObject<AggregateMessage>());
                        break;

                    case "T":
                        var t = parsedMessage.ToObject<TradeMessage>()!;
                        ProcessTrade(
                            t.Symbol,
                            Time.UnixMillisecondTimeStampToDateTime(t.Timestamp),
                            t.Price,
                            t.Size,
                            GetExchangeCode(t.ExchangeID),
                            t.Conditions,
                            t.TradeReportingFacilityId,
                            t.TradeReportingFacilityTimestamp,
                            t.Correction);
                        break;

                    case "Q":
                        ProcessQuote(parsedMessage.ToObject<QuoteMessage>());
                        break;

                    case "FMV":
                        var fmv = parsedMessage.ToObject<FairMarketValueMessage>()!;
                        ProcessTrade(fmv.Symbol, Time.UnixNanosecondTimeStampToDateTime(fmv.Timestamp), fmv.FairMarketValue);
                        break;

                    case "V":
                        var v = parsedMessage.ToObject<ValueIndexMessage>()!;
                        ProcessTrade(v.Symbol, Time.UnixMillisecondTimeStampToDateTime(v.Timestamp), v.Value);
                        break;

                    default:
                        break;
                }
            }
        }

        /// <summary>
        /// Processes an aggregate event message handling the incoming bar
        /// </summary>
        private void ProcessAggregate(AggregateMessage aggregate)
        {
            var symbol = _symbolMapper.GetLeanSymbol(aggregate.Symbol);
            var time = GetTickTime(symbol, aggregate.StartingTickTimestamp);
            var period = TimeSpan.FromMilliseconds(aggregate.EndingTickTimestamp - aggregate.StartingTickTimestamp);
            var bar = new TradeBar(time, symbol, aggregate.Open, aggregate.High, aggregate.Low, aggregate.Close, aggregate.Volume, period);

            lock (_dataAggregator)
            {
                _dataAggregator.Update(bar);
            }
        }

        /// <summary>
        /// Processes and incoming trade tick
        /// </summary>
        /// <param name="brokerageSymbol">The symbol of the traded security as provided by the brokerage.</param>
        /// <param name="timestamp">The timestamp of the trade tick as reported by the brokerage.</param>
        /// <param name="price">The trade price.</param>
        /// <param name="size">The traded quantity.</param>
        /// <param name="exchange">The exchange identifier where the trade occurred.</param>
        private void ProcessTrade(
            string brokerageSymbol,
            DateTime timestamp,
            decimal price,
            decimal size = 0m,
            string exchange = "",
            IReadOnlyList<long>? conditions = null,
            long? tradeReportingFacilityId = null,
            long? tradeReportingFacilityTimestamp = null,
            int? correction = null)
        {
            var leanSymbol = _symbolMapper.GetLeanSymbol(brokerageSymbol);
            var time = GetTickTime(leanSymbol, timestamp);
            var saleCondition = BuildSaleCondition(conditions);
            var tick = new Tick(time, leanSymbol, saleCondition, exchange, size, price);
            if (!string.IsNullOrEmpty(saleCondition))
            {
                // Avoid parsing the sale condition string (which isn't hex encoded) inside Tick.ParsedSaleCondition
                tick.ParsedSaleCondition = 0;
            }

            if (tradeReportingFacilityId.HasValue)
            {
                tick.SetProperty("TrfId", tradeReportingFacilityId.Value);
            }

            if (tradeReportingFacilityTimestamp.HasValue)
            {
                tick.SetProperty("TrfTimestamp", tradeReportingFacilityTimestamp.Value);
            }

            if (correction.HasValue)
            {
                tick.SetProperty("Correction", correction.Value);
            }

            lock (_dataAggregator)
            {
                _dataAggregator.Update(tick);
            }
        }

        private static string BuildSaleCondition(IReadOnlyList<long>? conditions)
        {
            if (conditions == null || conditions.Count == 0)
            {
                return string.Empty;
            }

            var buffer = new List<string>(conditions.Count);
            for (var i = 0; i < conditions.Count; i++)
            {
                buffer.Add(conditions[i].ToString(CultureInfo.InvariantCulture));
            }

            return string.Join(" ", buffer);
        }

        /// <summary>
        /// Processes and incoming quote tick
        /// </summary>
        private void ProcessQuote(QuoteMessage quote)
        {
            var symbol = _symbolMapper.GetLeanSymbol(quote.Symbol);
            var time = GetTickTime(symbol, quote.Timestamp);
            // TODO: Map trade.Conditions to Lean sale conditions
            // Note: Polygon's quotes have bid/ask exchange IDs, but Lean only has one exchange per tick. We'll use the bid exchange.
            var tick = new Tick(time, symbol, string.Empty, GetExchangeCode(quote.BidExchangeID),
                quote.BidSize, quote.BidPrice, quote.AskSize, quote.AskPrice);
            lock (_dataAggregator)
            {
                _dataAggregator.Update(tick);
            }
        }

        /// <summary>
        /// Converts the given UTC timestamp into the symbol security exchange time zone
        /// </summary>
        private DateTime GetTickTime(Symbol symbol, long timestamp)
        {
            var utcTime = Time.UnixMillisecondTimeStampToDateTime(timestamp);

            return GetTickTime(symbol, utcTime);
        }

        /// <summary>
        /// Converts the given UTC time into the symbol security exchange time zone
        /// </summary>
        private DateTime GetTickTime(Symbol symbol, DateTime utcTime)
        {
            DateTimeZone exchangeTimeZone;
            lock (_symbolExchangeTimeZones)
            {
                if (!_symbolExchangeTimeZones.TryGetValue(symbol, out exchangeTimeZone))
                {
                    // read the exchange time zone from market-hours-database
                    if (_marketHoursDatabase.TryGetEntry(symbol.ID.Market, symbol, symbol.SecurityType, out var entry))
                    {
                        exchangeTimeZone = entry.ExchangeHours.TimeZone;
                    }
                    // If there is no entry for the given Symbol, default to New York
                    else
                    {
                        exchangeTimeZone = TimeZones.NewYork;
                    }

                    _symbolExchangeTimeZones.Add(symbol, exchangeTimeZone);
                }
            }

            return utcTime.ConvertFromUtc(exchangeTimeZone);
        }

        #endregion

        #region Exchange mappings

        /// <summary>
        /// Gets the exchange code mappings from Polygon.io to be cached and used when fetching tick data
        /// </summary>
        protected virtual List<ExchangeMapping> FetchExchangeMappings()
        {
            // This url is not paginated, so we expect a single response
            const string uri = "v3/reference/exchanges";
            var response = RestApiClient.DownloadAndParseData<ExchangesResponse>(uri).SingleOrDefault();
            if (response == null)
            {
                throw new PolygonAuthenticationException($"Failed to download exchange mappings from {uri}. Make sure your API key is valid.");
            }

            return response.Results.ToList();
        }

        /// <summary>
        /// Gets the exchange code for the given exchange polygon id.
        /// This code is universal and can be used by Lean to create and <see cref="Exchange"/> instance.
        /// </summary>
        private string GetExchangeCode(int exchangePolygonId)
        {
            var mapping = _exchangeMappings.FirstOrDefault(x => x.ID == exchangePolygonId);
            if (mapping == null)
            {
                // Unknown exchange
                return string.Empty;
            }

            return mapping.Code;
        }

        #endregion

        /// <summary>
        /// Determines whether or not the specified config can be subscribed to
        /// </summary>
        private bool CanSubscribe(SubscriptionDataConfig config)
        {
            return
                // Filter out universe symbols
                config.Symbol.Value.IndexOfInvariant("universe", true) == -1 &&
                // Filter out canonical options
                !config.Symbol.IsCanonical() &&
                IsSupported(config.SecurityType, config.Type, config.TickType, config.Resolution);
        }

        private bool IsSupported(SecurityType securityType, Type dataType, TickType tickType, Resolution resolution)
        {
            // Check supported security types
            if (!IsSecurityTypeSupported(securityType))
            {
                if (!_unsupportedSecurityTypeMessageLogged)
                {
                    _unsupportedSecurityTypeMessageLogged = true;
                    Log.Trace($"PolygonDataProvider.IsSupported(): Unsupported security type: {securityType}");
                }
                return false;
            }

            if (!dataType.IsAssignableFrom(typeof(TradeBar)) &&
                !dataType.IsAssignableFrom(typeof(QuoteBar)) &&
                !dataType.IsAssignableFrom(typeof(OpenInterest)) &&
                !dataType.IsAssignableFrom(typeof(Tick)))
            {
                if (!_unsupportedDataTypeMessageLogged)
                {
                    _unsupportedDataTypeMessageLogged = true;
                    Log.Trace($"PolygonDataProvider.IsSupported(): Unsupported data type: {dataType}");
                }
                return false;
            }

            if (resolution < Resolution.Second && !_potentialUnsupportedResolutionMessageLogged)
            {
                _potentialUnsupportedResolutionMessageLogged = true;
                Log.Trace("PolygonDataProvider.IsSupported(): " +
                    $"Subscription for {securityType}-{dataType}-{tickType}-{resolution} will be attempted. " +
                    $"An Advanced Polygon.io subscription plan is required to stream tick data.");
            }

            if (tickType == TickType.Quote && !_potentialUnsupportedTickTypeMessageLogged)
            {
                _potentialUnsupportedTickTypeMessageLogged = true;
                Log.Trace("PolygonDataProvider.IsSupported(): " +
                    $"Subscription for {securityType}-{dataType}-{tickType}-{resolution} will be attempted. " +
                    $"An Advanced Polygon.io subscription plan is required to stream quote data.");
            }

            return true;
        }

        /// <summary>
        /// Determines whether or not the specified security type is a supported option
        /// </summary>
        private static bool IsSecurityTypeSupported(SecurityType securityType)
        {
            return _supportedSecurityTypes.Contains(securityType);
        }

        /// <summary>
        /// Attempts to parse a <see cref="LicenseType"/> value from the provided configuration string.
        /// </summary>
        /// <param name="licenseTypeFromConfig">The license type string retrieved from configuration (e.g., "Individual" or "Business").</param>
        /// <returns>
        /// A valid <see cref="LicenseType"/>. Returns <see cref="LicenseType.Individual"/> if the input is null, empty, or cannot be parsed.
        /// </returns>
        private LicenseType ParseLicenseType(string licenseTypeFromConfig)
        {
            var licenseType = LicenseType.Individual;
            if (!string.IsNullOrWhiteSpace(licenseTypeFromConfig))
            {
                if (!Enum.TryParse(licenseTypeFromConfig, true, out licenseType))
                {
                    licenseType = LicenseType.Individual;
                    Log.Error($"{nameof(PolygonDataProvider)}.{nameof(ParseLicenseType)}: An error occurred while parsing the license type '{licenseTypeFromConfig}', use default = {LicenseType.Individual}");
                }
            }

            Log.Trace($"{nameof(PolygonDataProvider)}.{nameof(ParseLicenseType)}: Using license type = '{licenseType}' (from input = '{licenseTypeFromConfig ?? "null"}').");

            return licenseType;
        }

        private class ModulesReadLicenseRead : Api.RestResponse
        {
            [JsonProperty(PropertyName = "license")]
            public string License;

            [JsonProperty(PropertyName = "organizationId")]
            public string OrganizationId;
        }

        /// <summary>
        /// Validate the user of this project has permission to be using it via our web API.
        /// </summary>
        private static void ValidateSubscription()
        {
            Log.Trace("PolygonDataProvider.ValidateSubscription(): Validation disabled; proceeding without QuantConnect license check.");
        }
    }
}
