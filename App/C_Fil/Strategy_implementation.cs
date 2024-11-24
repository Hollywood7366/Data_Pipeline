#region Using declarations
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Windows;
using System.Xml.Serialization;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Chart;
using NinjaTrader.Core.FloatingPoint;
using NinjaTrader.NinjaScript.Indicators;
using NinjaTrader.NinjaScript.Strategies;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using NinjaTrader.NinjaScript.DrawingTools;
using System.Windows.Media;
using NinjaTrader.NinjaScript.AddOns;

#endregion

/*
    Strategy Details:
    Symbol: @NQ
    Market 2: ---
    Market 3: ---
    Market 4: ---
    Start on: 1/3/2006 12:00:00 AM
    Stop on: 1/1/2025 4:59:59 AM
    Out of Sample %: 80 %
    Fitness Function: Sharpe
    Profit Target On: No
    Profit Multiple: 0
    Stop Loss On: No
    Stop Loss Multiple: 0
    Highest High On: No
    Highest High Lookback: 0
    Lowest Low On: No
    Lowest Low Lookback: 0
    Max Time: 7
    Profitable Closes: 1
*/

namespace NinjaTrader.NinjaScript.Strategies
{
    public class SlaveY : Strategy
    {
        #region Constants

        private const string StrategyName = "StrategyY";
        private const string StrategyVersion = "";

        private const string StrategyDescription = "Please add strategy description here.";
        private const int FirstValidValue = 256;
        private const string ProfxExitName = "ProfX";
        private const string TimexExitName = "TimeX";
        private const string MarketTypeFx = "FX";
        private const string MarketTypeFutures = "Fut";
        private const string MarketTypeOther = "Other";
        private const string KindFixed = "Fixed";
        private const string KindAtr = "ATR";
        private const string KindDefault = "Default";
        private const string EnterLongName = "Open Long";
        private const string EnterShortName = "Open Short";

        private const string StaticATRStopName = "ATR Stop";
        private const string StaticHhLlStopName = "HHLL Stop";
        private const string TrailATRStopName = "ATR Trail Stop";
        private const string DollarStopName = "Dollar Stop";
        private const string DollarTrailName = "Dollar Trail";
        private const string PercentStopName = "Percent Stop";
        private const string PercentTrailName = "Percent Trail";

        private const string StaticProfitTargetName = "Dollar Target";
        private const string AtrProfitTargetName = "ATR Profit Target";
        private const string HhLlProfitTargetName = "HHLL Profit Target";
        private const string PercentTargetName = "Percent Target";

        #endregion

        #region Variables

        private MyMarketPositionTypes _currentPositionType;

        public enum MyMarketPositionTypes
        {
            Long,
            Short,
            Both,
            None
        }

        private string _stopLossName;
        private string _profitTargetName;
        private string _entryName;

        private int _currentPosType;
        private int _prevPosType;
        private int _tradesToday;
        private int _profitCloseExitCounter;

        private double _prevAvgPrice;
        private double _startOfDayCumPnl;
        private double _pnlToday;
        private double _profitTarget;
        private double _stop;
        private double _profit;

        private bool _entryThisBar;
        private bool _exitInitiated;
        private bool _entrySignalFinal;
        private bool _isMaster;

        private string _symbol = "@NQ";
        private int _atrPeriod = 20;
        private int _sessoinTime = 160000;
        private int _startTime = 0;
        private int _endTime   = 235959;
        private int _maxTrades = 999999;
        private double _maxPnl = 999999;
        private double _minPnl = -999999;
        private string _mrkt   = "Fut";
        private string _kind   = "Default";
        private string _target = "USD";
        private bool _round    = true;
        private float _acct    = 100000;
        private float _margin  = 1974;
        private int _posSize   = 1;
        private bool _allowReEntryInBar = false;

        private double _dollarTrail;
        private double _tempTrail;

        private double _currentHighModeValue;
        private bool _isHighModeOn;
        private bool _isHighModeOnStarted;
        private double _currentLowModeValue;
        private bool _isLowModeOn;
        private bool _isLowModeOnStarted;

        #endregion

        #region Parameters

        [Display(Name = "Entry Direction", GroupName = "NinjaScriptParameters", Order = 0)]
        public MyMarketPositionTypes StrategyMarketPositionType { get; set; }

        [Display(Name = "Get slaves of ID", GroupName = "Master-Slave Settings", Order = 0)]
        public string SlaveIDs { get; set; }

        [Display(Name = "Request External Data", GroupName = "My Parameters", Order = 0)]
        public bool RequestExternalData { get; set; }

        [Display(Name = "Symbol_1", GroupName = "My Parameters", Order = 0)]
        public string SymbolOne { get; set; }

        [Display(Name = "Symbol_2", GroupName = "My Parameters", Order = 0)]
        public string SymbolTwo { get; set; }

        [Display(Name = "Symbol_3", GroupName = "My Parameters", Order = 0)]
        public string SymbolThree { get; set; }

        [Display(Name = "Symbol_4", GroupName = "My Parameters", Order = 0)]
        public string SymbolFour { get; set; }

        [Display(Name = "Symbol Period Type", GroupName = "My Parameters", Order = 0)]
        public BarsPeriodType SymbolPeriodType { get; set; }

        [Display(Name = "Symbol Period Length", GroupName = "My Parameters", Order = 0)]
        public int SymbolPeriodLength { get; set; }

        [Display(Name = "Delay Entry Bars", GroupName = "My Parameters", Order = 1)]
        public int DelayByBars { get; set; }


        #region Profit Targets

        [Display(Name = "Dollar Target", GroupName = "Profit Target Parameters", Order = 0)]
        public bool DollarTargetIsOn { get; set; }

        [Display(Name = "Dollar Target Value", GroupName = "Profit Target Parameters", Order = 1)]
        public double ProfitTargetDollars { get; set; }

        [Display(Name = "ATR Profit Target", GroupName = "Profit Target Parameters", Order = 2)]
        public bool AtrProfitTargetIsOn { get; set; }

        [Display(Name = "ATR Profit Target Coef", GroupName = "Profit Target Parameters", Order = 3)]
        [Range(0, int.MaxValue)]
        public double ProfitTargetCoef { get; set; }

        [Display(Name = "Percent Target", GroupName = "Profit Target Parameters", Order = 4)]
        public bool PercentTargetIsOn { get; set; }
		
        [Display(Name = "Percent Target Value", GroupName = "Profit Target Parameters", Order = 5)]
        public double PercentTargetValue { get; set; }

        [Display(Name = "LLHH Profit Target", GroupName = "Profit Target Parameters", Order = 6)]
        public bool LlHhProfitTargetIsOn { get; set; }

        [Display(Name = "LLHH Profit Period", GroupName = "Profit Target Parameters", Order = 7)]
        public int LlHhProfitPeriod { get; set; }

        #endregion

        #region StopLoss

        [Display(Name = "Dollar Stop", GroupName = "Stop Loss Parameters", Order = 0)]
        public bool DollarStop { get; set; }

        [Display(Name = "Dollar Stop Value", GroupName = "Stop Loss Parameters", Order = 1)]
        public double DollarStopValue { get; set; }

        [Display(Name = "Dollar Stop Trail", GroupName = "Stop Loss Parameters", Order = 2)]
        public bool DollarStopTrail { get; set; }

        [Display(Name = "Dollar Stop Trail Value", GroupName = "Stop Loss Parameters", Order = 3)]
        public double DollarStopTrailValue { get; set; }

        [Display(Name = "ATR Stop Loss", GroupName = "Stop Loss Parameters", Order = 4)]
        public bool StaticAtrStopLossIsOn { get; set; }

        [Display(Name = "ATR Stop Loss Coef", GroupName = "Stop Loss Parameters", Order = 5)]
        [Range(0,int.MaxValue)]
        public double StopLossCoef { get; set; }

        [Display(Name = "ATR Trailing Stop Loss", GroupName = "Stop Loss Parameters", Order = 6)]
        public bool TrailAtrStopLossIsOn { get; set; }

        [Display(Name = "ATR Trailing Stop Loss Coef", GroupName = "Stop Loss Parameters", Order = 7)]
        [Range(0, int.MaxValue)]
        public double TrailAtrStopLossCoef { get; set; }

        [Display(Name = "Percent Stop Loss", GroupName = "Stop Loss Parameters", Order = 8)]
        public bool PercentStopIsOn { get; set; }
		
        [Display(Name = "Percent Stop Value", GroupName = "Stop Loss Parameters", Order = 9)]
        public double PercentStopValue { get; set; }
		
        [Display(Name = "Percent Trail Loss", GroupName = "Stop Loss Parameters", Order = 10)]
        public bool PercentTrailIsOn { get; set; }
		
        [Display(Name = "Percent Trail Value", GroupName = "Stop Loss Parameters", Order = 11)]
        public double PercentTrailValue { get; set; }

        [Display(Name = "LLHH Stop Loss", GroupName = "Stop Loss Parameters", Order = 12)]
        public bool HhLlStopLossIsOn { get; set; }

        [Display(Name = "LLHH Stop Period", GroupName = "Stop Loss Parameters", Order = 13)]
        public int HhLlStopPeriod { get; set; }

        [Display(Name = "High + X stop/limit mode", GroupName = "Stop Loss Parameters", Order = 14)]
        public OneBarBuyMode HighMode { get; set; }

        [Display(Name = "X for High stop/limit mode", GroupName = "Stop Loss Parameters", Order = 15)]
        public double HighModeX { get; set; }

        [Display(Name = "Low + X stop/limit mode", GroupName = "Stop Loss Parameters", Order = 16)]
        public OneBarBuyMode LowMode { get; set; }

        [Display(Name = "X for Low stop/limit mode", GroupName = "Stop Loss Parameters", Order = 17)]
        public double LowModeX { get; set; }

        #endregion

        #region Exits

        [NinjaScriptProperty]
        [Display(Name = "Profit Close Exit", GroupName = "Exit Parameters", Order = 0)]
        public bool ProfitExit { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "Profitable Closes", GroupName = "Exit Parameters", Order = 1)]
        public int ProfitableCloses { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "Time Exit", GroupName = "Exit Parameters", Order = 2)]
        public bool TimeExit { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "Max Time", GroupName = "Exit Parameters", Order = 3)]
        public int Maxtime { get; set; }

        #endregion

        #endregion

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                InitializeSettings();
                SetDefaultParameters();
                
            }
            else if (State == State.Configure)
            {
                if (RequestExternalData)
                    AddExternalData();

                //Metrics = new double[0];
            }
            else if (State == State.DataLoaded)
            {
                
            }
        }

        private void InitializeSettings()
        {
            ClearOutputWindow();
            Description                  = StrategyDescription;
            Name                         = StrategyName + StrategyVersion;
            Calculate                    = Calculate.OnBarClose;
            EntriesPerDirection          = 100;
            EntryHandling                = EntryHandling.AllEntries;
            IsExitOnSessionCloseStrategy = true;
            ExitOnSessionCloseSeconds    = 30;
            IsFillLimitOnTouch           = false;
            MaximumBarsLookBack          = MaximumBarsLookBack.TwoHundredFiftySix;
            OrderFillResolution          = OrderFillResolution.Standard;
            Slippage                     = 1.45;
            StartBehavior                = StartBehavior.WaitUntilFlat;
            TimeInForce                  = TimeInForce.Gtc;
            TraceOrders                  = false;
            RealtimeErrorHandling        = RealtimeErrorHandling.StopCancelClose;
            StopTargetHandling           = StopTargetHandling.PerEntryExecution;
            BarsRequiredToTrade          = 255;
            IsInstantiatedOnEachOptimizationIteration = true; 
        }

        private void SetDefaultParameters()
        {
            TimeExit = true;
            ProfitExit = false;
            DollarTargetIsOn = false;
            AtrProfitTargetIsOn = false;
            StaticAtrStopLossIsOn = false;
            TrailAtrStopLossIsOn = false;
            DollarStopTrail = false;
            DollarStop = false;
            HhLlStopLossIsOn = false;
            LlHhProfitTargetIsOn = false;
            PercentTargetIsOn = false;
            PercentStopIsOn =  false;
            PercentTrailIsOn =  false;
            SymbolOne = "^VIX";
            SymbolTwo = "None";
            SymbolThree = "None";
            SymbolFour = "EURUSD";

            SymbolPeriodType = BarsPeriodType.Day;
            SymbolPeriodLength = 1;
            StrategyMarketPositionType = MyMarketPositionTypes.Long;
            ProfitableCloses = 1;
            Maxtime = 7;
            LlHhProfitPeriod = 0;
            HhLlStopPeriod = 0;
            DelayByBars = 0;
            StopLossCoef = 0;
            ProfitTargetCoef = 0;
            TrailAtrStopLossCoef = 0;
            ProfitTargetDollars = 0;
            DollarStopValue = 0;
            DollarStopTrailValue = 0;
			PercentStopValue = 0;
			PercentTargetValue = 0;
			PercentTrailValue = 0;
        }

        private void AddExternalData()
        {
            //AddDataSeries(SymbolOne, SymbolPeriodType, SymbolPeriodLength);
            //AddDataSeries(SymbolTwo, SymbolPeriodType, SymbolPeriodLength);
            //AddDataSeries(SymbolThree, SymbolPeriodType, SymbolPeriodLength);
            //AddDataSeries(SymbolFour, SymbolPeriodType, SymbolPeriodLength);
        }

        private bool _enterLong;
        private bool _enterShort;
        private bool _exitLong;
        private bool _exitShort;
        

        protected override void OnPositionUpdate(Position position, double averagePrice, int quantity, MarketPosition marketPosition)
        {
            TfSyncMasterSlave.WriteToSlaveFile(this, position, SlaveIDs);	
        }

        protected override void OnBarUpdate()
        {
            if (CurrentBar < FirstValidValue)
                return;

            if (BarsInProgress != 0)               
                return;


            SetGlobalPositionSize();
            CalculateDailyTradesAndPnl();
            SetEntrySignals();
            SetExitSignals();
            //SetRotationExitSignal();

            CheckHLModeOneBar();


            if (PositionIsActive(Position))
            {
                _delayLong = 0;
                _delayShort = 0;

                if (ProfitExit && ProfitableCloseExitIsActive())
                {
                    _profitCloseExitCounter =
                        IsLongOrShortInProfit() ? _profitCloseExitCounter + 1 : _profitCloseExitCounter;

                    if (_profitCloseExitCounter >= ProfitableCloses)
                        ExitMarket(ProfxExitName);
                }

                if (IsLong(Position) && _exitLong)
                    ExitMarket("Signal Exit");

                if (IsShort(Position) && _exitShort)
                    ExitMarket("Signal Exit");

                //if (rotation_signal > 0 && rotation_exit)
                //    ExitMarket("Rotation Exit");

                //// Ensemble
                //if (_ensembleExit)
                //    ExitMarket("Ensemble Exit");

                if (TimeExit && TimeToClosePosition())
                    ExitMarket(TimexExitName);

                CalculateStopTargets(false);
                CalculateProfitTargets(false);
                SetStopOrders();
                SetProfitOrders();
            }
            else
            {
                EntryModule();
            }
        }

        private void CheckHLModeOneBar()
        {

            if (IsFirstTickOfBar)
            {

                if (_isHighModeOnStarted)
                    _isHighModeOnStarted = false;

                if (_isLowModeOnStarted)
                    _isLowModeOnStarted = false;

                if (_isHighModeOn)
                {
                    _isHighModeOn = false;
                    _isHighModeOnStarted = true;

                    _currentHighModeValue = Bars.GetHigh(CurrentBar - 1) + HighModeX;
                }

                if (_isLowModeOn)
                {
                    _isLowModeOn = false;
                    _isLowModeOnStarted = true;

                    _currentLowModeValue = Bars.GetLow(CurrentBar - 1) - LowModeX;
                }
            }

            if (_isHighModeOnStarted)
            {
                switch (HighMode)
                {
                    case OneBarBuyMode.Limit:
                        if (IsLimit(_currentHighModeValue))
                        {
                            EnterLong(_posSize);
                            _isHighModeOnStarted = false;
                        }
                        break;
                    case OneBarBuyMode.Stop:
                        if (IsStop(_currentHighModeValue))
                        {
                            EnterLong(_posSize);
                            _isHighModeOnStarted = false;
                        }
                        break;
                }
            }

            if (_isLowModeOnStarted)
            {
                switch (LowMode)
                {
                    case OneBarBuyMode.Limit:
                        if (IsLimit(_currentLowModeValue))
                        {
                            EnterLong(_posSize);
                            _isLowModeOnStarted = false;
                        }
                        break;
                    case OneBarBuyMode.Stop:
                        if (IsStop(_currentLowModeValue))
                        {
                            EnterLong(_posSize);
                            _isLowModeOnStarted = false;
                        }
                        break;
                }
            }
        }

        private bool IsLimit(double value)
        {
            return Bars.GetAsk(CurrentBar) <= value;
        }

        private bool IsStop(double value)
        {
            return Bars.GetAsk(CurrentBar) >= value;
        }

        private int _delayLong;
        private int _delayShort;

        private void EntryModule()
        {
            if (!DelayedEntryIsOn())
                EntryNoDelay();

            if (DelayedEntryIsOn())
            {
                if (_delayLong > 0)
                {
                    _delayLong++;
                    _delayShort = 0;
                }

                if (_delayShort > 0)
                {
                    _delayShort++;
                    _delayLong = 0; 
                }

                if (_enterLong  && _delayLong == 0 && TradeLongOrBoth() && _delayShort ==0)
                {
                    _delayLong++;
                }

                if (_enterShort && _delayShort == 0 && TradeShortOrBoth() && _delayLong ==0)
                {
                    _delayShort++;
                }

                if (_delayLong > DelayByBars)
                {
                    EnterLongLimit(_posSize, Low[0], EnterLongName);
                    EnterHLModeOneBar();
                }

                if (_delayShort > DelayByBars)
                {
                    EnterShortLimit(_posSize, High[0], EnterShortName);
                    EnterHLModeOneBar();
                }
            }
        }

        private bool TradeLongOrBoth()
        {
            return StrategyMarketPositionType == MyMarketPositionTypes.Both ||
                   StrategyMarketPositionType == MyMarketPositionTypes.Long;
        }

        private bool TradeShortOrBoth()
        {
            return StrategyMarketPositionType == MyMarketPositionTypes.Both ||
                   StrategyMarketPositionType == MyMarketPositionTypes.Short;
        }

        private void EntryNoDelay()
        {
            if (StrategyMarketPositionType == MyMarketPositionTypes.Both)
            {
                if (_enterLong)
                {
                    EnterLongLimit(_posSize, Low[0], EnterLongName);
                    EnterHLModeOneBar();
                }

                if (_enterShort)
                {
                    EnterShortLimit(_posSize, High[0], EnterShortName);
                    EnterHLModeOneBar();
                }
            }

            if (StrategyMarketPositionType == MyMarketPositionTypes.Long)
            {
                if (_enterLong)
                {
                    EnterLongLimit(_posSize, Low[0], EnterLongName);
                    EnterHLModeOneBar();
                }
            }

            if (StrategyMarketPositionType == MyMarketPositionTypes.Short)
            {
                if (_enterShort)
                {
                    EnterShortLimit(_posSize, High[0], EnterShortName);
                    EnterHLModeOneBar();
                }
            }
        }

        private void EnterHLModeOneBar()
        {
            if (HighMode != OneBarBuyMode.None)
            {
                _isHighModeOn = true;
            }

            if (LowMode != OneBarBuyMode.None)
            {
                _isLowModeOn = true;
            }
        }

        private bool DelayedEntryIsOn()
        {
            return DelayByBars > 0;
        }

        private bool EntryTimeIsOk()
        {
            return ToTime(Time[0]) >= _startTime && ToTime(Time[0]) <= _endTime;
        }

        private bool EntryPnlIsOk()
        {
            return _pnlToday > _minPnl && _pnlToday < _maxPnl;
        }

        private bool EntryTradesTodayIsOk()
        {
            return _tradesToday < _maxTrades;
        }

        private void SetExitSignals()
        {
            _exitLong = ToTime(Time[0]) >= 160000;
            _exitShort = ToTime(Time[0]) >= 160000;
        }

        private void SetEntrySignals()
        {
            _enterLong = (ToDay(Time[0])>20060102 || (ToDay(Time[0])==20060102 && ToTime(Time[0])>=190000)) &&
                High[5] <= Close[8] && (ToTime(Time[0]) >= 0) && (ToTime(Time[0]) <= 10000) && Time[0].Month < 10;
            _enterLong &= ExtraEntryConditions();

            _enterShort = (ToDay(Time[0])>20060102 || (ToDay(Time[0])==20060102 && ToTime(Time[0])>=190000)) &&
                High[5] <= Close[8] && (ToTime(Time[0]) >= 0) && (ToTime(Time[0]) <= 10000) && Time[0].Month < 10;
            _enterShort &= ExtraEntryConditions();
        }
        private bool ExtraEntryConditions()
        {
            return EntryTimeIsOk() && EntryPnlIsOk() && EntryTradesTodayIsOk();
        }

        private bool TimeToClosePosition()
        {
            var entryName = GetEntryName();
            if (entryName == "") return false;
            return Maxtime > 0 && BarsSinceEntryExecution(BarsInProgress, entryName, 0) >= Maxtime - 1;
        }

        private bool ProfitableCloseExitIsActive()
        {
            var entryName = GetEntryName();
            if (entryName == "") return false;
            return ProfitableCloses > 0 && BarsSinceEntryExecution(BarsInProgress, entryName, 0) > 0;
        }

        private string GetEntryName()
        {
            if (_currentPositionType == MyMarketPositionTypes.Long)
            {
                return EnterLongName;
            }
            if (_currentPositionType == MyMarketPositionTypes.Short)
            {
                return EnterShortName;
            }

            return "";
        }

        private bool IsLongOrShortInProfit()
        {
            return IsLong(Position) && LongPositionInProfit() || IsShort(Position) && ShortPositionInProfit();
        }

        private bool LongPositionInProfit()
        {
            return Close[0] >= Position.AveragePrice;
        }

        private bool ShortPositionInProfit()
        {
            return Close[0] <= Position.AveragePrice;
        }

        private void CalculateDailyTradesAndPnl()
        {
            _currentPosType = Position.MarketPosition == MarketPosition.Long ? 1 : 0;

            if (IsNewTrade())
                _tradesToday += 1;

            _pnlToday = SystemPerformance.AllTrades.TradesPerformance.Currency.CumProfit - _startOfDayCumPnl;
            _prevPosType = _currentPosType;
            _prevAvgPrice = Position.AveragePrice;

            if (IsNewDay())
            {
                _tradesToday = 0;
                _pnlToday = 0;
                _startOfDayCumPnl = SystemPerformance.AllTrades.TradesPerformance.Currency.CumProfit;
            }
        }

        public bool PositionIsActive(Position position)
        {
            return position.MarketPosition != MarketPosition.Flat;
        }

        public bool IsLong(Position position)
        {
            return position.MarketPosition == MarketPosition.Long;
        }

        public bool IsShort(Position position)
        {
            return position.MarketPosition == MarketPosition.Short;
        }

        private bool IsNewTrade()
        {
            return _currentPosType == 1 && _prevPosType != 1 ||
                   (_currentPosType == 1 && _prevPosType == 1 &&
                    Position.AveragePrice != _prevAvgPrice);
        }

        private bool IsNewDay()
        {
            return Time[0].Day != Time[1].Day && IsFirstTickOfBar;
        }

        private void SetGlobalPositionSize()
        {
            _posSize = 0;

            double atr = AFATR(_atrPeriod)[0];
            double risk = _acct * .01;
            double conversion = 0;
            double type = StaticAtrStopLossIsOn ? 0.00 : 1;

            if (_mrkt == MarketTypeFx)
                SetFxPositionSize(risk, atr, type, conversion);
            
            else if (_mrkt == MarketTypeFutures)
                SetFuturesPositionSize(risk, atr, type);
           
            else if (_mrkt == MarketTypeOther)
                SetOtherPositionSize(risk, atr, type);
        }

        private void SetFxPositionSize(double risk, double atr, double type, double conversion)
        {
            var symbStartThree = _symbol.Substring(0, 3);
            var symbEndThree = _symbol.Substring(3, 3);
            var marketStartThree = SymbolFour.Substring(0, 3);
            var marketEndThree = SymbolFour.Substring(3, 3);

            if (_kind == KindFixed) // to convert need XXXUSD
            {
                if (symbStartThree == marketEndThree)
                    conversion = 1 / Closes[4][0];

                else if (symbStartThree == marketStartThree && marketEndThree == _target)
                    conversion = Closes[4][0];

                else if (symbStartThree == marketStartThree && marketEndThree == _target)
                    conversion = 1;

                _posSize = (int)(_acct / conversion);
            }
            else if (_kind == KindAtr) // to convert need USDYYY
            {
                if (marketEndThree == _target)
                    conversion = 1;

                else if (marketStartThree == _target)
                    conversion = Closes[4][0];

                else if (symbEndThree == marketStartThree && marketEndThree == _target)
                    conversion = 1 / Closes[4][0];

                _posSize = (int)(risk * conversion / (atr * type));
            }
            else if (_kind == KindDefault)
            {
                _posSize = 100000;
            }
        }

        private void SetFuturesPositionSize(double risk, double atr, double type)
        {
            if (_kind == KindFixed)
                _posSize = (int)(_acct / _margin);

            else if (_kind == KindAtr)
                _posSize = (int)(risk / (atr * type * Instrument.MasterInstrument.PointValue));

            else if (_kind == KindDefault)
                _posSize = 1;
        }

        private void SetOtherPositionSize(double risk, double atr, double type)
        {
            if (_kind == KindFixed)
                _posSize = (int)(_acct / Close[0]);

            else if (_kind == KindAtr)
                _posSize = (int)(risk / (atr * type));

            else if (_kind == KindDefault)
                _posSize = 100;
        }

        protected override void OnExecutionUpdate(Execution execution, string executionId, double price, int quantity,
            MarketPosition marketPosition, string orderId, DateTime time)
            {
                if (OrderFilled(execution.Order))
                {
                    if (IsEntryOrder(execution.Order))
                    {
                        _delayLong = 0;
                        _delayShort = 0;

                        SetCurrentPosType(execution.Order);
                        CalculateStopTargets(true);
                        CalculateProfitTargets(true);
                        SetStopOrders();
                        SetProfitOrders();
                    
                }
                    else
                    {
                        _profitCloseExitCounter = 0;
                        SetCurrentPosType(execution.Order);

                        if (ImmediateReEntry(execution.Order))
                        {
                            EntryModule();
                        }
                    }
                }
            }

        private bool ImmediateReEntry(Order order)
        {
            return order.Name == ProfxExitName || order.Name == TimexExitName || order.Name == "Signal Exit";
        }

        private void SetCurrentPosType(Order order)
        {
            if (order.Name == EnterLongName)
                _currentPositionType = MyMarketPositionTypes.Long;

            else if (order.Name == EnterShortName)
                _currentPositionType = MyMarketPositionTypes.Short;

            else
            {
                _currentPositionType = MyMarketPositionTypes.None;
            }
        }

        double _staticProfit = 0;
        double _atrProfit = 0;
        double _hHlLTarget = 0;

        private void CalculateProfitTargets(bool isAtEntryFill)
        {
            
            if (_currentPositionType == MyMarketPositionTypes.Long)
            {
                if (DollarTargetIsOn && isAtEntryFill)
                {
                    _staticProfit = Position.AveragePrice + (ProfitTargetDollars / (Instrument.MasterInstrument.PointValue * Position.Quantity));
                    //_staticProfit = Close[0] + (ProfitTargetDollars / (Instrument.MasterInstrument.PointValue * Position.Quantity));
                    
                }
                if (AtrProfitTargetIsOn && isAtEntryFill)
                {
                    _atrProfit = Close[0] + AFATR(_atrPeriod)[0] * ProfitTargetCoef;

                }

				if (PercentTargetIsOn && isAtEntryFill)
					_atrProfit = Close[0] * (1 + (PercentTargetValue / 100));

                if (LlHhProfitTargetIsOn)
                {
                    _hHlLTarget = MAX(High, LlHhProfitPeriod)[0];
                    
                }

                _profit = MinOfThree(_staticProfit, _atrProfit, _hHlLTarget);
                SetProfitTargetName(_staticProfit, _atrProfit, _hHlLTarget);
            }

            else if (_currentPositionType == MyMarketPositionTypes.Short)
            {
                if (DollarTargetIsOn && isAtEntryFill)
                {
                    _staticProfit = Position.AveragePrice - (ProfitTargetDollars / (Instrument.MasterInstrument.PointValue * Position.Quantity));
                    //_staticProfit = Close[0] - (ProfitTargetDollars / (Instrument.MasterInstrument.PointValue * Position.Quantity));
                }
                if (AtrProfitTargetIsOn && isAtEntryFill)
                {
                    _atrProfit = Close[0] - AFATR(_atrPeriod)[0] * ProfitTargetCoef;
                    
                }

                if (PercentTargetIsOn && isAtEntryFill)
                    _atrProfit = Close[0] * (1 - (PercentTargetValue/100));

                if (LlHhProfitTargetIsOn)
                {
                    _hHlLTarget = MIN(Low, LlHhProfitPeriod)[0];
                }

                _profit = MaxOfThree(_staticProfit, _atrProfit, _hHlLTarget);
                SetProfitTargetName(_staticProfit, _atrProfit, _hHlLTarget);
            }
        }

        double _stopAtr = 0;
        double _stopHhLl = 0;
        double _stopTrailAtr = 0;
        private double _dollarStop;

        private void CalculateStopTargets(bool isAtEntryFill)
        {
            if (!AnyStopIsOn())
                return;

            if (_currentPositionType == MyMarketPositionTypes.Long)
            {
                if (StaticAtrStopLossIsOn && isAtEntryFill)
                {
                    _stopAtr = Close[0] - AFATR(_atrPeriod)[0] * StopLossCoef; 
                }
                if (DollarStop && isAtEntryFill)
                {
                    _dollarStop = Position.AveragePrice - (DollarStopValue / (Instrument.MasterInstrument.PointValue * Position.Quantity));
                    //_dollarStop = Close[0] - (DollarStopValue / (Instrument.MasterInstrument.PointValue * Position.Quantity));
                }
                if (HhLlStopLossIsOn)
                {
                    _stopHhLl = MIN(Low, HhLlStopPeriod)[0];
                }
                if (TrailAtrStopLossIsOn)
                {
                    _stopTrailAtr = Close[0] - AFATR(_atrPeriod)[0] * TrailAtrStopLossCoef;

                    if (!isAtEntryFill)
                        _stopTrailAtr = Math.Max(_stop, _stopTrailAtr);
                }
                if (DollarStopTrail)
                {
                    _tempTrail = High[0] - (DollarStopTrailValue / (Instrument.MasterInstrument.PointValue * Position.Quantity));

                    if (BarsSinceEntryExecution() == 0)
                    {
                        _tempTrail = 0;
                        _dollarTrail = Position.AveragePrice - (DollarStopTrailValue / (Instrument.MasterInstrument.PointValue * Position.Quantity));
                    }

                    if (_tempTrail > _dollarTrail)
                        _dollarTrail = _tempTrail;

                    if (!isAtEntryFill)
                    {
                        var newValue = Close[0] - (DollarStopTrailValue / (Instrument.MasterInstrument.PointValue * Position.Quantity));
                        _dollarTrail = Math.Max(_stop, newValue);
                    }
                }

                if (PercentStopIsOn && isAtEntryFill)
                {
                    _stopAtr = Close[0] * ( 1 - (PercentStopValue / 100.0));
                }

                if (PercentTrailIsOn)
                {
					
                    _tempTrail = Close[0] * (1 - (PercentTrailValue/100.0));
					
                    if (BarsSinceEntryExecution() == 0)
                    {
                        _tempTrail = 0;
                        _stopTrailAtr = Close[0] * (1 - (PercentTrailValue / 100.0));
                     }

                    if (_tempTrail > _stopTrailAtr)
                        _stopTrailAtr = _tempTrail;
					
                    if (!isAtEntryFill)
                    {
                        var newTrail = Close[0] * (1 - (PercentTrailValue / 100.0));
                        _stopTrailAtr = Math.Max(newTrail, _stop);
                    }	
                }

                _stop = MaxOfAll(_stopAtr, _stopHhLl, _stopTrailAtr, _dollarStop, _dollarTrail);
                SetStopLossName(_stopAtr, _stopHhLl, _stopTrailAtr, _dollarStop, _dollarTrail);
            }

            else if (_currentPositionType == MyMarketPositionTypes.Short)
            {
                if (StaticAtrStopLossIsOn && isAtEntryFill)
                {
                    _stopAtr = Close[0] + AFATR(_atrPeriod)[0] * StopLossCoef;
                }
                if (DollarStop && isAtEntryFill)
                {
                    _dollarStop = Position.AveragePrice + (DollarStopValue / (Instrument.MasterInstrument.PointValue * Position.Quantity));
                    //_dollarStop = Close[0] + (DollarStopValue / (Instrument.MasterInstrument.PointValue * Position.Quantity));
                }
                if (HhLlStopLossIsOn)
                {
                    _stopHhLl = MAX(High, HhLlStopPeriod)[0];
                }
                if (TrailAtrStopLossIsOn)
                {
                    _stopTrailAtr = Close[0] + AFATR(_atrPeriod)[0] * TrailAtrStopLossCoef;

                    if (!isAtEntryFill)
                        _stopTrailAtr = Math.Min(_stop, _stopTrailAtr);
                }
                if (DollarStopTrail)
                {
                    _tempTrail = Low[0] + (DollarStopTrailValue / (Instrument.MasterInstrument.PointValue * Position.Quantity));
                    
                    if (BarsSinceEntryExecution() == 0)
                    {
                        _tempTrail = 0;
                        _dollarTrail = Position.AveragePrice + (DollarStopTrailValue / (Instrument.MasterInstrument.PointValue * Position.Quantity));
                    }
                    
                    if (_tempTrail < _dollarTrail)
                        _dollarTrail = _tempTrail;

                    if (!isAtEntryFill)
                    {
                        var newValue = Close[0] + (DollarStopTrailValue / (Instrument.MasterInstrument.PointValue * Position.Quantity));
                        _dollarTrail = Math.Min(_stop, newValue);
                    }
                }

                if (PercentStopIsOn)
                {
                    _stopAtr = Close[0] * ( 1 + (PercentStopValue / 100.0));
                }

                if (PercentTrailIsOn)
                {
					
                    _tempTrail = Close[0] * (1 + (PercentTrailValue/100.0));
					
                    if (BarsSinceEntryExecution() == 0)
                    {
                        _tempTrail = 999999;
                        _stopTrailAtr = Close[0] * (1 + (PercentTrailValue / 100.0));
                    }

                    if (_tempTrail < _stopTrailAtr)
                        _stopTrailAtr = _tempTrail;
					
                    if (!isAtEntryFill)
                    {
                        var newTrail = Close[0] * (1 + (PercentTrailValue / 100.0));
                        _stopTrailAtr = Math.Min(newTrail, _stop);
                    }	
                }

                _stop = MinOfAll(_stopAtr, _stopHhLl, _stopTrailAtr, _dollarStop, _dollarTrail);
                SetStopLossName(_stopAtr, _stopHhLl, _stopTrailAtr, _dollarStop, _dollarTrail);
            }
        }

        private void SetStopLossName(double stopAtr, double stopHhLl, double stopTrailAtr, double dollarStop, double dollarTrail)
        {
            if (_stop == stopAtr)
            {
                _stopLossName = StaticATRStopName;
                if (PercentStopIsOn)
                    _stopLossName = PercentStopName;
            }

            if (_stop == stopHhLl)
                _stopLossName = StaticHhLlStopName;

            if (_stop == stopTrailAtr)
            {
                _stopLossName = TrailATRStopName;
                if (PercentTrailIsOn)
                    _stopLossName = PercentTrailName;
            }

            if (_stop == dollarStop)
                _stopLossName = DollarStopName;

            if (_stop == dollarTrail)
                _stopLossName = DollarTrailName;
        }

        private void SetProfitTargetName(double staticProfit, double atrProfit, double hHlLTarget)
        {
            if (_profit == staticProfit)
                _profitTargetName = StaticProfitTargetName;

            if (_profit == atrProfit)
            {
                _profitTargetName = AtrProfitTargetName;
                if (PercentTargetIsOn)
                    _profitTargetName = PercentTargetName;
            }

            if (_profit == hHlLTarget)
                _profitTargetName = HhLlProfitTargetName;
        }

        private double MaxOfThree(double valOne, double valTwo, double valThree)
        {
            return Math.Max(valOne, Math.Max(valTwo, valThree));
        }

        private double MaxOfAll(params double[] vals)
        {

            var values = new List<double>();

            foreach (var number in vals)
            {
                values.Add(number);
            }

            if (values.Count == 0)
                return 0;

            var maxVal = values.Max();
            return maxVal;
        }

        private double MinOfThree(double valOne, double valTwo, double valThree)
        {
            var values = new List<double>()
            {   
                valOne, valTwo, valThree
            };

            var nonZeroValues = values.Where(v => v != 0);

            if (nonZeroValues.Count() == 0)
                return 0;

            var minNonZero = nonZeroValues.Min();
            return minNonZero;
        }

        private double MinOfAll(params double[] vals)
        {
            var values = new List<double>();

            foreach (var number in vals)
            {
                values.Add(number);
            }

            var nonZeroValues = values.Where(v => v != 0);

            if (nonZeroValues.Count() == 0)
                return 0;

            var minNonZero = nonZeroValues.Min();
            return minNonZero;
        }

        private void SetStopOrders()
        {
            if (!AnyStopIsOn() || _stop == 0 || _entryName == "")
                return;

            if (IsLong(Position))
            {
                if(_stop >= Position.AveragePrice)
                    _stop = Position.AveragePrice - 1.0 * TickSize;
                ExitLongStopMarket(_stop, _stopLossName, EnterLongName);
            }

            if (IsShort(Position))
            {
                if(_stop <= Position.AveragePrice)
                    _stop = Position.AveragePrice + 1.0 * TickSize;
                ExitShortStopMarket(_stop, _stopLossName, EnterShortName); 
            }
        }

        private void SetProfitOrders()
        {
            if (!AnyProfitIsOn() || _profit == 0 || _entryName == "")
                return;

            if (IsLong(Position))
            {
                if(_profit <= Position.AveragePrice)
                    _profit= Position.AveragePrice + 1.0 * TickSize;
                ExitLongLimit(_profit, _profitTargetName, EnterLongName);
            }
         
            if (IsShort(Position))
            {
                if(_profit >= Position.AveragePrice)
                    _profit = Position.AveragePrice - 1.0 * TickSize;  
                ExitShortLimit(_profit, _profitTargetName, EnterShortName); 
            }    
        }

        private bool AnyStopIsOn()
        {
            return StaticAtrStopLossIsOn || HhLlStopLossIsOn || TrailAtrStopLossIsOn || DollarStop ||
                   DollarStopTrail || PercentStopIsOn || PercentTrailIsOn;
        }

        private bool AnyProfitIsOn()
        {
            return DollarTargetIsOn || AtrProfitTargetIsOn || LlHhProfitTargetIsOn || PercentTargetIsOn;
        }

        private bool IsEntryOrder(Order order)
        {
            if (order.Name == EnterLongName || order.Name == EnterShortName)
                return true;

            return false;
        }

        public bool OrderFilled(Order order)
        {
            return order.OrderState == OrderState.Filled;
        }

        private void ExitMarket(string exitName)
        {
            if (IsLong(Position))
                ExitLong(exitName, EnterLongName);

            else if (IsShort(Position))
                ExitShort(exitName, EnterShortName);
        }
    }

    public enum OneBarBuyMode
    { 
        None,
        Limit,
        Stop
    }
}
