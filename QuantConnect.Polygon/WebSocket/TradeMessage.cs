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

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Models a Polygon.io WebSocket trade API message
    /// </summary>
    public class TradeMessage : BaseMessage
    {
        /// <summary>
        /// The exchange ID
        /// </summary>
        [JsonProperty("x")]
        public int ExchangeID { get; set; }

        /// <summary>
        /// The trade price
        /// </summary>
        [JsonProperty("p")]
        public decimal Price { get; set; }

        /// <summary>
        /// The trade size
        /// </summary>
        [JsonProperty("s")]
        public long Size { get; set; }

        /// <summary>
        /// The trade conditions
        /// </summary>
        [JsonProperty("c")]
        public long[] Conditions { get; set; }

        /// <summary>
        /// The trade reporting facility identifier (if applicable)
        /// </summary>
        [JsonProperty("trfi")]
        public long? TradeReportingFacilityId { get; set; }

        /// <summary>
        /// The trade reporting facility timestamp in milliseconds since Unix epoch (if applicable)
        /// </summary>
        [JsonProperty("trft")]
        public long? TradeReportingFacilityTimestamp { get; set; }

        /// <summary>
        /// Correction code, when the trade is marked as a correction.
        /// Polygon's websocket payloads currently emit this as the short-form field 'e';
        /// older payloads may surface a 'correction' field instead, so we accept both.
        /// </summary>
        [JsonProperty("correction")]
        public int? Correction { get; set; }

        [JsonProperty("e")]
        private JToken LegacyCorrection
        {
            set => Correction = ParseCorrection(value);
        }

        private static int? ParseCorrection(JToken token)
        {
            if (token == null || token.Type == JTokenType.Null)
            {
                return null;
            }

            if (token.Type == JTokenType.Integer)
            {
                return token.Value<int>();
            }

            if (token.Type == JTokenType.String)
            {
                var text = token.Value<string>();
                if (string.IsNullOrWhiteSpace(text))
                {
                    return null;
                }
                if (int.TryParse(text, out var parsed))
                {
                    return parsed;
                }
            }

            return null;
        }
    }
}
