#region Using declarations
using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using NinjaTrader.Cbi;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.Strategies;
using Newtonsoft.Json.Linq;
#endregion

namespace NinjaTrader.NinjaScript.Strategies
{
    /// <summary>
    /// HighLowSignalStrategy — receives breadth regime signals from HighLow TUI
    /// via TCP and trades NQ based on score thresholds.
    ///
    /// Entry:
    ///   Long  when score >= +ScoreThreshold AND regime is "Bull Trend" or "Bull Reversal"
    ///   Short when score <= -ScoreThreshold AND regime is "Bear Trend" or "Bear Reversal"
    ///
    /// Exit (whichever comes first):
    ///   1. Profit target: ProfitTargetPoints (default 5 NQ points = 20 ticks)
    ///   2. Stop loss:     StopLossPoints     (default 10 NQ points = 40 ticks)
    ///   3. Time exit:     TimeExitSeconds     (default 15 seconds wall-clock)
    ///   4. Opposing signal (bull position + bear signal, or vice versa)
    ///   5. Idle regime
    /// </summary>
    public class HighLowSignalStrategy : Strategy
    {
        // ── Signal fields (written by background thread, read by OnBarUpdate) ──
        // C# volatile does not support double; use a lock for thread-safe access.
        private readonly object _signalLock = new object();
        private double  _score;
        private string  _regime        = "Idle";
        private double  _imbalance1m;
        private double  _imbalance5m;
        private double  _imbalance20m;
        private double  _imbalance30s;
        private double  _impulse;
        private int     _expansion1m;
        private double  _expansionRate;
        private double  _volumeBreadth;
        private double  _ratio1m;
        private double  _signalTimestamp;
        private bool    _regimeChanged;   // set true by ParseSignal when regime transitions
        private string  _prevRegime = "Idle";
        private volatile bool    _connected;

        // ── Internal state ──
        private Thread     _readerThread;
        private TcpClient  _tcpClient;
        private volatile bool _shutdown;
        private DateTime   _entryTime = DateTime.MinValue;
        private bool       _inPosition;
        private string     _lastEntryRegime = "Idle";  // tracks regime at last entry to prevent re-entry on same signal

        #region Strategy Parameters

        [NinjaScriptProperty]
        [Display(Name = "Signal Host", Description = "HighLow TUI TCP host", Order = 1, GroupName = "Signal Connection")]
        public string SignalHost { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "Signal Port", Description = "HighLow TUI TCP port", Order = 2, GroupName = "Signal Connection")]
        public int SignalPort { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "Score Threshold", Description = "Minimum |score| to enter (0.0-1.0)", Order = 3, GroupName = "Entry")]
        public double ScoreThreshold { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "Profit Target (points)", Description = "Profit target in instrument points", Order = 4, GroupName = "Exit")]
        public double ProfitTargetPoints { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "Stop Loss (points)", Description = "Stop loss in instrument points", Order = 5, GroupName = "Exit")]
        public double StopLossPoints { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "Time Exit (seconds)", Description = "Max seconds in trade before flat exit", Order = 6, GroupName = "Exit")]
        public int TimeExitSeconds { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "Verbose Logging", Description = "Print every signal to Output window", Order = 7, GroupName = "Debug")]
        public bool VerboseLogging { get; set; }

        #endregion

        protected override void OnStateChange()
        {
            switch (State)
            {
                case State.SetDefaults:
                    Name                     = "HighLowSignalStrategy";
                    Description              = "Trades NQ based on HighLow TUI breadth signals via TCP";
                    Calculate                = Calculate.OnEachTick;
                    EntriesPerDirection      = 1;
                    EntryHandling            = EntryHandling.AllEntries;
                    IsExitOnSessionCloseStrategy = true;
                    ExitOnSessionCloseSeconds    = 30;
                    IsFillLimitOnTouch       = false;
                    MaximumBarsLookBack      = MaximumBarsLookBack.TwoHundredFiftySix;
                    OrderFillResolution      = OrderFillResolution.Standard;
                    IsInstantiatedOnEachOptimizationIteration = true;

                    // Defaults
                    SignalHost           = "127.0.0.1";
                    SignalPort           = 9137;
                    ScoreThreshold       = 0.70;
                    ProfitTargetPoints   = 5;
                    StopLossPoints       = 10;
                    TimeExitSeconds      = 15;
                    VerboseLogging       = false;
                    break;

                case State.DataLoaded:
                    // Set managed profit target and stop loss (in ticks)
                    // NQ tick size = 0.25, so points * 4 = ticks
                    SetProfitTarget(CalculationMode.Ticks, ProfitTargetPoints * 4);
                    SetStopLoss(CalculationMode.Ticks, StopLossPoints * 4);

                    // Start background TCP reader
                    _shutdown = false;
                    _readerThread = new Thread(ReaderLoop)
                    {
                        IsBackground = true,
                        Name = "HighLowSignalReader"
                    };
                    _readerThread.Start();
                    break;

                case State.Terminated:
                    _shutdown = true;
                    try { _tcpClient?.Close(); } catch { }
                    try { _readerThread?.Join(3000); } catch { }
                    break;
            }
        }

        protected override void OnBarUpdate()
        {
            // Only trade on the primary series and when we have enough bars
            if (BarsInProgress != 0 || CurrentBars[0] < 1)
                return;

            // Read latest signal snapshot under lock
            double score;
            string regime;
            bool regimeChanged;
            bool connected = _connected;
            lock (_signalLock)
            {
                score         = _score;
                regime        = _regime ?? "Idle";
                regimeChanged = _regimeChanged;
                _regimeChanged = false;  // consume the flag
            }

            // ── Time-based exit ──
            if (Position.MarketPosition != MarketPosition.Flat && _inPosition)
            {
                TimeSpan elapsed = DateTime.UtcNow - _entryTime;
                if (elapsed.TotalSeconds >= TimeExitSeconds)
                {
                    if (VerboseLogging)
                        Print("HighLow: Time exit after " + elapsed.TotalSeconds.ToString("F1") + "s");
                    ExitPosition();
                    return;
                }
            }

            // ── Signal-based exits ──
            if (Position.MarketPosition != MarketPosition.Flat && _inPosition)
            {
                // Exit on Idle
                if (regime == "Idle")
                {
                    if (VerboseLogging)
                        Print("HighLow: Exit on Idle regime");
                    ExitPosition();
                    return;
                }

                // Exit long on opposing bear signal
                if (Position.MarketPosition == MarketPosition.Long
                    && (regime == "Bear Trend" || regime == "Bear Reversal"))
                {
                    if (VerboseLogging)
                        Print("HighLow: Exit long on opposing bear signal (" + regime + ")");
                    ExitPosition();
                    return;
                }

                // Exit short on opposing bull signal
                if (Position.MarketPosition == MarketPosition.Short
                    && (regime == "Bull Trend" || regime == "Bull Reversal"))
                {
                    if (VerboseLogging)
                        Print("HighLow: Exit short on opposing bull signal (" + regime + ")");
                    ExitPosition();
                    return;
                }
            }

            // ── Entry logic (only on NEW regime change, when flat and connected) ──
            if (Position.MarketPosition != MarketPosition.Flat || !connected)
                return;

            // Only enter when the regime just changed AND it's different from what we last traded
            if (!regimeChanged || regime == _lastEntryRegime)
                return;

            // Long entry
            if (score >= ScoreThreshold
                && (regime == "Bull Trend" || regime == "Bull Reversal"))
            {
                if (VerboseLogging)
                    Print("HighLow: Enter LONG — regime=" + regime + " score=" + score.ToString("F3"));

                EnterLong("HighLowLong");
                _entryTime = DateTime.UtcNow;
                _inPosition = true;
                _lastEntryRegime = regime;
            }
            // Short entry
            else if (score <= -ScoreThreshold
                && (regime == "Bear Trend" || regime == "Bear Reversal"))
            {
                if (VerboseLogging)
                    Print("HighLow: Enter SHORT — regime=" + regime + " score=" + score.ToString("F3"));

                EnterShort("HighLowShort");
                _entryTime = DateTime.UtcNow;
                _inPosition = true;
                _lastEntryRegime = regime;
            }
        }

        protected override void OnExecutionUpdate(Execution execution, string executionId,
            double price, int quantity, MarketPosition marketPosition,
            string orderId, DateTime time)
        {
            // Track when we go flat (profit target, stop loss, or manual exit)
            if (Position.MarketPosition == MarketPosition.Flat)
            {
                _inPosition = false;
                _lastEntryRegime = "";  // allow re-entry on next regime change
            }
        }

        private void ExitPosition()
        {
            if (Position.MarketPosition == MarketPosition.Long)
                ExitLong("HighLowLong");
            else if (Position.MarketPosition == MarketPosition.Short)
                ExitShort("HighLowShort");
            _inPosition = false;
        }

        // ── Background TCP reader ──────────────────────────────────────────

        private void ReaderLoop()
        {
            int backoffMs = 1000;
            const int maxBackoffMs = 15000;

            while (!_shutdown)
            {
                try
                {
                    _tcpClient = new TcpClient();
                    _tcpClient.Connect(SignalHost, SignalPort);
                    _connected = true;
                    backoffMs = 1000; // reset on successful connect

                    Print("HighLow: Connected to signal server " + SignalHost + ":" + SignalPort);

                    using (var stream = _tcpClient.GetStream())
                    using (var reader = new StreamReader(stream, System.Text.Encoding.UTF8))
                    {
                        string line;
                        while (!_shutdown && (line = reader.ReadLine()) != null)
                        {
                            line = line.Trim();
                            if (string.IsNullOrEmpty(line))
                                continue; // heartbeat

                            try
                            {
                                ParseSignal(line);
                            }
                            catch (Exception ex)
                            {
                                if (VerboseLogging)
                                    Print("HighLow: Parse error: " + ex.Message);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (!_shutdown)
                    {
                        Print("HighLow: Connection lost — " + ex.Message + ". Retrying in " + backoffMs + "ms");
                    }
                }
                finally
                {
                    _connected = false;
                    try { _tcpClient?.Close(); } catch { }
                    _tcpClient = null;
                }

                if (!_shutdown)
                {
                    Thread.Sleep(backoffMs);
                    backoffMs = Math.Min(backoffMs * 2, maxBackoffMs);
                }
            }
        }

        private void ParseSignal(string json)
        {
            var obj = JObject.Parse(json);

            lock (_signalLock)
            {
                _score           = obj.Value<double>("score");
                string newRegime = obj.Value<string>("regime") ?? "Idle";
                if (newRegime != _prevRegime)
                {
                    _regimeChanged = true;
                    _prevRegime    = newRegime;
                }
                _regime          = newRegime;
                _imbalance1m     = obj.Value<double>("imbalance_1m");
                _imbalance5m     = obj.Value<double>("imbalance_5m");
                _imbalance20m    = obj.Value<double>("imbalance_20m");
                _imbalance30s    = obj.Value<double>("imbalance_30s");
                _impulse         = obj.Value<double>("impulse");
                _expansion1m     = obj.Value<int>("expansion_1m");
                _expansionRate   = obj.Value<double>("expansion_rate");
                _volumeBreadth   = obj.Value<double>("volume_breadth");
                _ratio1m         = obj.Value<double>("ratio_1m");
                _signalTimestamp = obj.Value<double>("ts");
            }

            if (VerboseLogging)
            {
                lock (_signalLock)
                {
                    Print("HighLow: " + _regime + " score=" + _score.ToString("F3")
                        + " imp=" + _impulse.ToString("F3")
                        + " exp=" + _expansion1m);
                }
            }
        }
    }
}
