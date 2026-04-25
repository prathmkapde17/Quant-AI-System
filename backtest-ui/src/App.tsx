import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { 
  BarChart3, 
  TrendingUp, 
  ArrowDownCircle, 
  Zap, 
  History, 
  Settings2,
  LineChart as LineChartIcon,
  ChevronRight,
  Database,
  Activity,
  ArrowUpRight,
  RefreshCw,
  Search
} from 'lucide-react';
import { 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer,
  AreaChart,
  Area
} from 'recharts';

// Types
interface BacktestMetrics {
  total_return_pct: number;
  cagr_pct: number;
  max_drawdown_pct: number;
  sharpe_ratio: number;
  sortino_ratio: number;
  calmar_ratio: number;
  win_rate_pct: number;
  profit_factor: number;
  total_trades: number;
  avg_win_pct: number;
  avg_loss_pct: number;
  recovery_factor: number;
}

interface BacktestResponse {
  id: string;
  metrics: BacktestMetrics;
  equity_curve: number[];
  trades: any[];
}

const API_BASE = "http://localhost:8000";

function App() {
  const [symbols, setSymbols] = useState<string[]>([]);
  const [selectedSymbol, setSelectedSymbol] = useState("BTCUSDT");
  const [strategy, setStrategy] = useState("rsi_mean_reversion");
  const [params, setParams] = useState({ oversold: 30, overbought: 70 });
  const [result, setResult] = useState<BacktestResponse | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetchSymbols();
  }, []);

  const fetchSymbols = async () => {
    try {
      const resp = await axios.get(`${API_BASE}/api/symbols`);
      setSymbols(resp.data.symbols);
    } catch (e) {
      console.error("Failed to fetch symbols", e);
    }
  };

  const runBacktest = async () => {
    setLoading(true);
    try {
      const resp = await axios.post(`${API_BASE}/api/backtest/run`, {
        symbol: selectedSymbol,
        exchange: "binance",
        timeframe: "1m",
        strategy_name: strategy,
        params: params,
        days: 7
      });
      setResult(resp.data);
    } catch (e) {
      alert("Backtest failed: " + (e as any).response?.data?.detail || "Unknown error");
    } finally {
      setLoading(false);
    }
  };

  const equityData = result?.equity_curve.map((v, i) => ({ step: i, equity: v })) || [];

  return (
    <div className="flex h-screen bg-[#020617] text-slate-100 overflow-hidden font-sans">
      {/* Sidebar - Integrated Design */}
      <aside className="w-80 bg-slate-950 border-r border-slate-800/60 flex flex-col z-30">
        <div className="p-8 border-b border-slate-800/40">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-indigo-600 shadow-lg shadow-indigo-600/30">
              <Activity className="w-6 h-6 text-white" />
            </div>
            <div>
              <h1 className="text-lg font-bold leading-tight tracking-tight">Antigravity</h1>
              <p className="text-[10px] font-medium text-slate-500 uppercase tracking-[0.2em]">Quant Suite v1.0</p>
            </div>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto p-6 space-y-8 scrollbar-hide">
          {/* Market Selection */}
          <section>
            <h3 className="text-[11px] font-bold text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
              <Database className="w-3 h-3" /> Universe Selection
            </h3>
            <div className="space-y-4">
              <div className="relative group">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-600 group-focus-within:text-indigo-400 transition-colors" />
                <select 
                  value={selectedSymbol}
                  onChange={(e) => setSelectedSymbol(e.target.value)}
                  className="w-full bg-slate-900 border border-slate-800 rounded-xl pl-10 pr-4 py-3 text-sm font-medium focus:ring-2 focus:ring-indigo-500/50 focus:border-indigo-500 transition-all appearance-none cursor-pointer hover:border-slate-700"
                >
                  {symbols.map(s => <option key={s} value={s}>{s}</option>)}
                </select>
              </div>
            </div>
          </section>

          {/* Algorithm Config */}
          <section className="space-y-6">
            <h3 className="text-[11px] font-bold text-slate-500 uppercase tracking-widest flex items-center gap-2">
              <Settings2 className="w-3 h-3" /> Logic Configuration
            </h3>
            
            <div className="space-y-4">
              <div className="p-4 bg-slate-900/50 rounded-2xl border border-slate-800/50">
                <label className="text-xs font-semibold text-slate-400 block mb-3 uppercase tracking-wider">Strategy Model</label>
                <select 
                  value={strategy}
                  onChange={(e) => setStrategy(e.target.value)}
                  className="w-full bg-slate-950 border border-slate-800 rounded-lg p-2.5 text-sm font-medium focus:outline-none focus:border-indigo-500"
                >
                  <option value="rsi_mean_reversion">RSI Mean Reversion</option>
                  <option value="ema_cross">EMA Cross Trend</option>
                  <option value="macd_cross">MACD Crossover</option>
                </select>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <ParamInput 
                  label="Buy Threshold" 
                  value={params.oversold} 
                  onChange={(v) => setParams({...params, oversold: v})} 
                />
                <ParamInput 
                  label="Sell Threshold" 
                  value={params.overbought} 
                  onChange={(v) => setParams({...params, overbought: v})} 
                />
              </div>
            </div>
          </section>
        </div>

        {/* Action Area */}
        <div className="p-6 border-t border-slate-800/40 bg-slate-950/50">
          <button 
            onClick={runBacktest}
            disabled={loading}
            className="group relative w-full bg-indigo-600 hover:bg-indigo-500 disabled:bg-slate-800 text-white font-bold py-4 rounded-2xl transition-all flex items-center justify-center gap-3 overflow-hidden shadow-xl shadow-indigo-900/20 active:scale-95"
          >
            <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/10 to-white/0 -translate-x-full group-hover:translate-x-full transition-transform duration-1000" />
            {loading ? (
              <RefreshCw className="w-5 h-5 animate-spin" />
            ) : (
              <>
                <span className="text-sm">Initiate Backtest</span>
                <ChevronRight className="w-4 h-4 group-hover:translate-x-1 transition-transform" />
              </>
            )}
          </button>
        </div>
      </aside>

      {/* Main Content Area */}
      <main className="flex-1 flex flex-col relative overflow-hidden bg-[radial-gradient(ellipse_at_top_right,_var(--tw-gradient-stops))] from-slate-900/20 via-transparent to-transparent">
        {/* Navbar */}
        <header className="h-20 px-10 flex items-center justify-between border-b border-slate-800/40 relative z-10 backdrop-blur-xl bg-slate-950/40">
          <div className="flex items-center gap-8">
            <div>
              <span className="text-[10px] font-bold text-slate-500 uppercase tracking-[0.2em] block mb-1">Active Scenario</span>
              <h2 className="text-xl font-bold text-white flex items-center gap-2">
                {selectedSymbol} <span className="text-indigo-500/50 px-2 font-mono text-sm">/</span> 1-Minute
              </h2>
            </div>
            <div className="h-8 w-[1px] bg-slate-800" />
            <div className="flex gap-4">
              <StatusBadge label="Historical" value="30 Days" />
              <StatusBadge label="Features" value="Active" />
            </div>
          </div>

          <div className="flex items-center gap-3">
             <button className="flex items-center gap-2 px-4 py-2 bg-slate-900 border border-slate-800 rounded-xl text-sm font-medium hover:bg-slate-800 transition-colors">
               <History className="w-4 h-4 text-slate-400" /> Archive
             </button>
          </div>
        </header>

        {/* Scrollable Workspace */}
        <div className="flex-1 overflow-y-auto p-10 space-y-10 custom-scrollbar">
          {/* Key Metrics Grid */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 xl:grid-cols-5 gap-6">
            <StatsCard 
              label="Cumulative Return" 
              value={result?.metrics.total_return_pct} 
              format="%" 
              trend={result?.metrics.total_return_pct}
              icon={<TrendingUp />}
            />
            <StatsCard 
              label="Ann. Return (CAGR)" 
              value={result?.metrics.cagr_pct} 
              format="%" 
              icon={<Zap />}
            />
            <StatsCard 
              label="Max Drawdown" 
              value={result?.metrics.max_drawdown_pct} 
              format="%" 
              isNegative 
              icon={<ArrowDownCircle />}
            />
            <StatsCard 
              label="Sharpe Ratio" 
              value={result?.metrics.sharpe_ratio} 
              format="" 
              icon={<BarChart3 />}
            />
            <StatsCard 
              label="Win Rate" 
              value={result?.metrics.win_rate_pct} 
              format="%" 
              icon={<Activity />}
            />
          </div>

          {/* Primary Visualization Area */}
          <div className="grid grid-cols-12 gap-8">
            <div className="col-span-12 xl:col-span-12 glass-panel rounded-[2rem] p-10 overflow-hidden relative glow-card">
              <div className="flex items-center justify-between mb-10">
                <div className="flex items-center gap-4">
                  <div className="p-3 bg-indigo-500/10 rounded-2xl">
                    <LineChartIcon className="w-6 h-6 text-indigo-400" />
                  </div>
                  <div>
                    <h3 className="text-xl font-bold text-white">Equity Performance</h3>
                    <p className="text-sm text-slate-500">Capital growth across simulation steps</p>
                  </div>
                </div>
                {result && (
                  <div className="px-5 py-2 bg-emerald-500/10 border border-emerald-500/20 rounded-full flex items-center gap-2">
                    <ArrowUpRight className="w-4 h-4 text-emerald-400" />
                    <span className="text-sm font-bold text-emerald-400">Final: ${result.equity_curve[result.equity_curve.length-1].toLocaleString(undefined, {maximumFractionDigits: 0})}</span>
                  </div>
                )}
              </div>

              <div className="h-[450px] w-full">
                {result ? (
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={equityData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                      <defs>
                        <linearGradient id="equityGrad" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#6366f1" stopOpacity={0.4}/>
                          <stop offset="95%" stopColor="#6366f1" stopOpacity={0}/>
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} opacity={0.5} />
                      <XAxis dataKey="step" hide />
                      <YAxis 
                        domain={['auto', 'auto']} 
                        stroke="#475569" 
                        fontSize={11} 
                        fontWeight={600}
                        tickFormatter={(v) => `$${v.toLocaleString()}`}
                        axisLine={false}
                        tickLine={false}
                      />
                      <Tooltip 
                        contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '16px', boxShadow: '0 20px 25px -5px rgb(0 0 0 / 0.5)' }}
                        itemStyle={{ color: '#6366f1', fontWeight: 700 }}
                        labelStyle={{ display: 'none' }}
                      />
                      <Area 
                        type="monotone" 
                        dataKey="equity" 
                        stroke="#6366f1" 
                        strokeWidth={3}
                        fillOpacity={1} 
                        fill="url(#equityGrad)" 
                        animationDuration={1500}
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="h-full w-full flex flex-col items-center justify-center border-2 border-dashed border-slate-800 rounded-3xl text-slate-500/40 transition-all group hover:border-indigo-500/30">
                    <div className="w-20 h-20 bg-slate-900 rounded-full flex items-center justify-center mb-6 shadow-inner">
                       <LineChartIcon className="w-8 h-8 opacity-40 group-hover:text-indigo-400 transition-colors" />
                    </div>
                    <p className="font-medium tracking-wide">Configure and run a strategy to start analysis</p>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}

// Sub-Components for Clean Code
function ParamInput({ label, value, onChange }: { label: string, value: number, onChange: (v: number) => void }) {
  return (
    <div className="bg-slate-900 border border-slate-800 p-4 rounded-2xl hover:border-slate-700 transition-colors">
      <span className="text-[10px] font-bold text-slate-500 uppercase tracking-widest block mb-2">{label}</span>
      <input 
        type="number" 
        value={value}
        onChange={(e) => onChange(parseInt(e.target.value))}
        className="w-full bg-transparent text-xl font-bold text-white focus:outline-none"
      />
    </div>
  )
}

function StatusBadge({ label, value }: { label: string, value: string }) {
  return (
    <div className="bg-slate-900/80 px-4 py-2 rounded-xl border border-slate-800/50 flex flex-col">
      <span className="text-[9px] font-bold text-slate-500 uppercase tracking-widest">{label}</span>
      <span className="text-xs font-bold text-indigo-400">{value}</span>
    </div>
  )
}

function StatsCard({ label, value, format, trend, isNegative, icon }: any) {
  const displayVal = value !== undefined ? value.toFixed(2) : "0.00";
  
  return (
    <div className="glass-panel p-7 rounded-[2rem] hover:translate-y-[-4px] transition-all duration-300 group">
      <div className="flex items-center justify-between mb-4">
        <div className={`p-2 rounded-xl bg-slate-950 border border-slate-800 group-hover:border-indigo-500/50 transition-colors ${isNegative ? 'text-rose-400' : 'text-indigo-400'}`}>
          {React.cloneElement(icon, { size: 18 })}
        </div>
        {trend !== undefined && (
          <div className={`text-[10px] font-bold px-2 py-0.5 rounded-full ${trend >= 0 ? 'bg-emerald-500/10 text-emerald-400' : 'bg-rose-500/10 text-rose-400'}`}>
            {trend >= 0 ? '+' : ''}{trend.toFixed(1)}%
          </div>
        )}
      </div>
      <div className="space-y-1">
        <span className="text-[11px] font-bold text-slate-500 uppercase tracking-wider">{label}</span>
        <div className={`text-3xl font-black tracking-tight ${isNegative ? 'text-rose-400' : 'text-slate-100'}`}>
          {displayVal}{format}
        </div>
      </div>
    </div>
  )
}

export default App;
