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

using MathNet.Numerics.Statistics;
using QuantConnect.Data;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.Securities;
using System;
using System.Collections.Generic;
using System.Linq;

namespace QuantConnect.Algorithm.Framework.Alphas
{
    /// <summary>
    /// This alpha model is designed to rank every pair combination by its pearson correlation 
    /// and trade the pair with the hightest correlation
    /// This model generates alternating long ratio/short ratio insights emitted as a group
    /// </summary>
    public class PearsonCorrelationPairsTradingAlphaModel : BasePairsTradingAlphaModel
    {
        private readonly int _lookback;
        private readonly Resolution _resolution;
        private readonly double _minimumCorrelation;
        private Tuple<Symbol, Symbol> _bestPair;

        /// <summary>
        /// Initializes a new instance of the <see cref="PearsonCorrelationPairsTradingAlphaModel"/> class
        /// </summary>
        /// <param name="lookback">Lookback period of the analysis</param>
        /// <param name="resolution">Analysis resolution</param>
        /// <param name="threshold">The percent [0, 100] deviation of the ratio from the mean before emitting an insight</param>
        /// <param name="minimumCorrelation">The minimum correlation to consider a tradable pair</param>
        public PearsonCorrelationPairsTradingAlphaModel(int lookback, Resolution resolution, decimal threshold = 1m, double minimumCorrelation = .5)
            : base(lookback, resolution, threshold)
        {
            _lookback = lookback;
            _resolution = resolution;
            _minimumCorrelation = minimumCorrelation;
        }

        /// <summary>
        /// Event fired each time the we add/remove securities from the data feed
        /// </summary>
        /// <param name="algorithm">The algorithm instance that experienced the change in securities</param>
        /// <param name="changes">The security additions and removals from the algorithm</param>
        public override void OnSecuritiesChanged(QCAlgorithmFramework algorithm, SecurityChanges changes)
        {
            NotifiedSecurityChanges.UpdateCollection(Securities, changes);

            var symbols = Securities.Select(x => x.Symbol).ToArray();

            var history = algorithm.History(symbols, _lookback, _resolution);

            var vectors = GetPriceVectors(history);

            if (vectors.Count() == 0)
            {
                algorithm.Debug($"PearsonCorrelationPairsTradingAlphaModel.OnSecuritiesChanged(): The requested historical data does not have series of prices with the same date/time. Please consider increasing the looback period. Current lookback: {_lookback}");
            }
            else
            {
                var pearsonMatrix = Correlation.PearsonMatrix(vectors).UpperTriangle();

                var maxValue = pearsonMatrix.Enumerate().Where(x => Math.Abs(x) < 1).Max();
                if (maxValue >= _minimumCorrelation)
                {
                    var maxTuple = pearsonMatrix.Find(x => x == maxValue);
                    _bestPair = Tuple.Create(symbols[maxTuple.Item1], symbols[maxTuple.Item2]);
                }
            }

            base.OnSecuritiesChanged(algorithm, changes);
        }

        /// <summary>
        /// Check whether the assets pass a pairs trading test
        /// </summary>
        /// <param name="algorithm">The algorithm instance that experienced the change in securities</param>
        /// <param name="asset1">The first asset's symbol in the pair</param>
        /// <param name="asset2">The second asset's symbol in the pair</param>
        /// <returns>True if the statistical test for the pair is successful</returns>
        public override bool HasPassedTest(QCAlgorithmFramework algorithm, Symbol asset1, Symbol asset2)
        {
            return _bestPair != null && asset1 == _bestPair.Item1 && asset2 == _bestPair.Item2;
        }

        private IEnumerable<double[]> GetPriceVectors(IEnumerable<Slice> slices)
        {
            var symbols = Securities.Select(x => x.Symbol).ToArray();
            var timeZones = Securities.ToDictionary(x => x.Symbol, y => y.Exchange.TimeZone);
            var isSingleTimeZone = timeZones.Values.Distinct().Count() == 1;

            if (isSingleTimeZone)
            {
                return slices
                    .Where(x => x.Count == symbols.Length)
                    .SelectMany(x => x.Values)
                    .GroupBy(x => x.Symbol)
                    .Select(x =>
                    {
                        var array = x.Select(b => Math.Log((double)b.Price)).ToArray();
                        for (var i = array.Length - 1; i > 0; i--)
                        {
                            array[i] = array[i] - array[i - 1];
                        }
                        array[0] = array[1];
                        return array;
                    });
            }
            else
            {
                return slices
                    .SelectMany(x => x.Values.Select(d =>
                    {
                        var utcEndTime = d.EndTime.ConvertToUtc(timeZones[d.Symbol]);
                        if (_resolution == Resolution.Daily)
                        {
                            utcEndTime = utcEndTime.Date;
                        }
                        return Tuple.Create(utcEndTime, d.Symbol, d.Price);
                    }))
                    // Group by UTC time and only accept data from days with all symbols
                    .GroupBy(x => x.Item1)
                    .Where(x => x.Count() == symbols.Length)
                    .SelectMany(x => x.Select(y => y))
                    // Group by Symbol and return an array of LogPrice difference
                    .GroupBy(x => x.Item2)
                    .Select(x =>
                    {
                        var array = x.Select(b => Math.Log((double)b.Item3)).ToArray();
                        for (var i = array.Length - 1; i > 0; i--)
                        {
                            array[i] = array[i] - array[i - 1];
                        }
                        array[0] = array[1];
                        return array;
                    });
            }
        }
    }
}