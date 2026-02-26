import React, { useState, useEffect } from 'react';
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';

// --- Types ---
interface SystemHealth {
  [key: string]: boolean;
}

interface GlobalStats {
  total_rows: number;
  active_vehicles: number;
  avg_latency: number;
  dlq_backlog: number;
}

interface Vehicle {
  vehicle_id: string;
  rows_processed: number;
  rejected_rows: number;
  validation_rate: number;
  avg_latency: number;
  last_seen_sec: number;
  latest_payload: any;
  module_payloads: Record<string, any>;
  history: Record<string, {
    timestamps: string[];
    metrics: Record<string, number[]>;
  }>;
}

interface Snapshot {
  system_health: SystemHealth;
  global_stats: GlobalStats;
  vehicles: Vehicle[];
}

export default function DashboardObserver() {
  const [data, setData] = useState<Snapshot | null>(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<'leaderboard' | 'analytics' | 'forensics' | 'inspector'>('leaderboard');

  // Forensics State
  const [forensicsVid, setForensicsVid] = useState<string>('');
  const [forensicsMod, setForensicsMod] = useState<string>('');

  // Inspector State
  const [inspectorVid, setInspectorVid] = useState<string>('');
  const [inspectorSource, setInspectorSource] = useState<string>('ALL (Latest)');

  useEffect(() => {
    const fetchData = async () => {
      try {
        const res = await fetch('http://127.0.0.1:8005/api/observer/snapshot');
        if (res.ok) {
          const json = await res.json();
          setData(json);
          
          // Auto-select first vehicle if not set
          if (json.vehicles && json.vehicles.length > 0) {
            if (!forensicsVid) setForensicsVid(json.vehicles[0].vehicle_id);
            if (!inspectorVid) setInspectorVid(json.vehicles[0].vehicle_id);
          }
        }
      } catch (err) {
        console.error("Observer fetch error", err);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 1500); // Poll every 1.5s
    return () => clearInterval(interval);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  if (loading && !data) return <div className="p-6 text-gray-400 font-mono">INITIALIZING TELEMETRY STREAM...</div>;
  if (!data || !data.vehicles || data.vehicles.length === 0) {
    return (
      <div className="p-6 text-gray-400 font-mono border border-gray-700 bg-gray-900 m-4">
        [SYS_WARN] WAITING FOR TELEMETRY STREAM. START REPLAY SERVICE.
      </div>
    );
  }

  const { system_health, global_stats, vehicles } = data;

  // --- Render Helpers ---
  const selectedForensicsV = vehicles.find(v => v.vehicle_id === forensicsVid) || vehicles[0];
  const availableForensicsMods = selectedForensicsV?.history ? Object.keys(selectedForensicsV.history) : [];
  
  // Auto-select module if current is invalid
  const currentForensicsMod = availableForensicsMods.includes(forensicsMod) ? forensicsMod : availableForensicsMods[0];

  const selectedInspectorV = vehicles.find(v => v.vehicle_id === inspectorVid) || vehicles[0];
  const availableInspectorSources = ["ALL (Latest)", ...(selectedInspectorV?.module_payloads ? Object.keys(selectedInspectorV.module_payloads) : [])];

  return (
    <div className="p-4 bg-[#0a0a0a] min-h-screen text-gray-200 font-sans">
      <h1 className="text-2xl font-bold mb-4 tracking-wider uppercase border-b border-gray-700 pb-2">
        📡 Telemetry Command Center
      </h1>

      {/* TOP BAR: SYSTEM HEALTH */}
      <div className="grid grid-cols-2 md:grid-cols-6 gap-2 mb-6">
        {Object.entries(system_health).map(([name, isUp]) => (
          <div key={name} className={`p-2 text-center border font-mono text-xs font-bold ${isUp ? 'bg-green-900/30 border-green-700 text-green-400' : 'bg-red-900/30 border-red-700 text-red-400'}`}>
            <div className="uppercase mb-1 text-[10px] text-gray-400">{name}</div>
            <div>{isUp ? 'ACTIVE' : 'OFFLINE'}</div>
          </div>
        ))}
      </div>

      {/* GLOBAL KPIS */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <KpiCard title="Total Throughput" value={global_stats.total_rows.toLocaleString()} />
        <KpiCard title="Active Fleet Size" value={global_stats.active_vehicles} />
        <KpiCard title="Global Latency" value={`${global_stats.avg_latency.toFixed(1)} ms`} color="text-yellow-400" />
        <KpiCard title="DLQ Backlog" value={global_stats.dlq_backlog} color={global_stats.dlq_backlog > 0 ? "text-red-400" : "text-gray-200"} />
      </div>

      {/* TABS MENU */}
      <div className="flex space-x-1 mb-4 border-b border-gray-700">
        <TabButton id="leaderboard" label="🏆 Leaderboard" active={activeTab === 'leaderboard'} onClick={setActiveTab} />
        <TabButton id="analytics" label="📊 Fleet Analytics" active={activeTab === 'analytics'} onClick={setActiveTab} />
        <TabButton id="forensics" label="🛠️ Module Forensics" active={activeTab === 'forensics'} onClick={setActiveTab} />
        <TabButton id="inspector" label="🔍 Live Inspector" active={activeTab === 'inspector'} onClick={setActiveTab} />
      </div>

      {/* TAB CONTENT */}
      <div className="bg-[#121212] border border-gray-800 p-4">
        
        {/* TAB 1: LEADERBOARD */}
        {activeTab === 'leaderboard' && (
          <div className="overflow-x-auto">
            <table className="w-full text-left font-mono text-sm">
              <thead className="bg-gray-800 text-gray-400 text-xs uppercase border-b border-gray-700">
                <tr>
                  <th className="p-3">Vehicle ID</th>
                  <th className="p-3">Rows Processed</th>
                  <th className="p-3">Rejected Rows</th>
                  <th className="p-3">Quality Score</th>
                  <th className="p-3">Avg Latency</th>
                  <th className="p-3">Last Seen</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-800">
                {vehicles.map((v) => (
                  <tr key={v.vehicle_id} className="hover:bg-gray-800/50 transition-colors">
                    <td className="p-3 font-bold text-blue-400">{v.vehicle_id}</td>
                    <td className="p-3">{v.rows_processed.toLocaleString()}</td>
                    <td className="p-3 text-red-400">{v.rejected_rows.toLocaleString()}</td>
                    <td className="p-3">
                      <div className="flex items-center">
                        <span className="w-10">{v.validation_rate.toFixed(1)}%</span>
                        <div className="ml-2 w-24 h-2 bg-gray-700 rounded-full overflow-hidden">
                          <div className={`h-full ${v.validation_rate > 95 ? 'bg-green-500' : 'bg-red-500'}`} style={{ width: `${v.validation_rate}%` }} />
                        </div>
                      </div>
                    </td>
                    <td className="p-3 text-yellow-400">{v.avg_latency.toFixed(1)} ms</td>
                    <td className="p-3 text-gray-500">{v.last_seen_sec.toFixed(1)}s ago</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* TAB 2: FLEET ANALYTICS */}
        {activeTab === 'analytics' && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="border border-gray-800 p-4 bg-gray-900/50">
              <h3 className="text-xs font-bold text-gray-400 uppercase mb-4 tracking-widest">Latency Heatmap (ms)</h3>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={vehicles}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" vertical={false} />
                    <XAxis dataKey="vehicle_id" stroke="#888" tick={{ fill: '#888', fontSize: 12 }} />
                    <YAxis stroke="#888" tick={{ fill: '#888', fontSize: 12 }} />
                    <Tooltip contentStyle={{ backgroundColor: '#1e1e1e', borderColor: '#333' }} />
                    <Bar dataKey="avg_latency" fill="#facc15" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>

            <div className="border border-gray-800 p-4 bg-gray-900/50">
              <h3 className="text-xs font-bold text-gray-400 uppercase mb-4 tracking-widest">Data Quality Distribution</h3>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={vehicles}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" vertical={false} />
                    <XAxis dataKey="vehicle_id" stroke="#888" tick={{ fill: '#888', fontSize: 12 }} />
                    <YAxis stroke="#888" tick={{ fill: '#888', fontSize: 12 }} />
                    <Tooltip contentStyle={{ backgroundColor: '#1e1e1e', borderColor: '#333' }} />
                    <Legend />
                    <Bar dataKey="rows_processed" name="Accepted" stackId="a" fill="#2e7d32" />
                    <Bar dataKey="rejected_rows" name="Rejected" stackId="a" fill="#c62828" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>
        )}

        {/* TAB 3: MODULE FORENSICS */}
        {activeTab === 'forensics' && (
          <div className="flex flex-col md:flex-row gap-6">
            {/* Sidebar Controls */}
            <div className="w-full md:w-64 flex-shrink-0 space-y-4 border-r border-gray-800 pr-4">
              <div>
                <label className="block text-xs text-gray-500 uppercase font-bold mb-1">Target Vehicle</label>
                <select 
                  className="w-full bg-gray-900 border border-gray-700 text-gray-200 font-mono text-sm p-2 focus:outline-none focus:border-blue-500"
                  value={forensicsVid}
                  onChange={e => setForensicsVid(e.target.value)}
                >
                  {vehicles.map(v => <option key={v.vehicle_id} value={v.vehicle_id}>{v.vehicle_id}</option>)}
                </select>
              </div>

              <div>
                <label className="block text-xs text-gray-500 uppercase font-bold mb-1">Module Component</label>
                <div className="space-y-1">
                  {availableForensicsMods.map(mod => (
                    <button
                      key={mod}
                      onClick={() => setForensicsMod(mod)}
                      className={`w-full text-left px-3 py-2 text-sm font-mono uppercase border ${currentForensicsMod === mod ? 'bg-blue-900/40 border-blue-500 text-blue-300' : 'bg-gray-800 border-gray-700 text-gray-400 hover:bg-gray-700'}`}
                    >
                      {mod}
                    </button>
                  ))}
                </div>
              </div>
            </div>

            {/* Main Graphs Area */}
            <div className="flex-grow">
              {currentForensicsMod && selectedForensicsV?.history?.[currentForensicsMod] ? (() => {
                const hist = selectedForensicsV.history[currentForensicsMod];
                const metricsEntries = Object.entries(hist.metrics);
                
                return (
                  <div>
                    <h3 className="text-lg font-mono uppercase mb-4 text-blue-400">
                      {forensicsVid} // {currentForensicsMod}
                    </h3>
                    
                    {/* Top 4 KPIs */}
                    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
                      {metricsEntries.slice(0, 4).map(([mName, mVals]) => (
                        <div key={mName} className="border border-gray-800 bg-gray-900 p-3">
                          <div className="text-[10px] text-gray-500 uppercase truncate" title={mName}>{mName.replace(/_/g, ' ')}</div>
                          <div className="text-xl font-mono text-gray-200 mt-1">
                            {mVals.length > 0 ? Number(mVals[mVals.length - 1]).toFixed(2) : '0'}
                          </div>
                        </div>
                      ))}
                    </div>

                    {/* Line Charts Grid */}
                    <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
                      {metricsEntries.map(([mName, mVals]) => {
                        // Construct Recharts friendly array
                        const chartData = hist.timestamps.map((ts, idx) => ({
                          time: ts,
                          value: mVals[idx]
                        })).slice(-50); // Show last 50 points for density

                        return (
                          <div key={mName} className="border border-gray-800 bg-[#0f0f0f] p-2">
                            <div className="text-xs font-mono text-gray-400 mb-2 uppercase pl-2">{mName}</div>
                            <div className="h-40">
                              <ResponsiveContainer width="100%" height="100%">
                                <LineChart data={chartData}>
                                  <CartesianGrid strokeDasharray="3 3" stroke="#222" vertical={false} />
                                  <XAxis dataKey="time" stroke="#555" tick={{ fill: '#555', fontSize: 10 }} />
                                  <YAxis stroke="#555" tick={{ fill: '#555', fontSize: 10 }} domain={['auto', 'auto']} />
                                  <Tooltip 
                                    contentStyle={{ backgroundColor: '#111', borderColor: '#333', fontSize: '12px' }}
                                    itemStyle={{ color: '#00e5ff' }}
                                  />
                                  <Line type="stepAfter" dataKey="value" stroke="#00acc1" strokeWidth={2} dot={false} isAnimationActive={false} />
                                </LineChart>
                              </ResponsiveContainer>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              })() : <div className="text-gray-500 font-mono text-sm mt-10">NO HISTORY AVAILABLE FOR SELECTED MODULE.</div>}
            </div>
          </div>
        )}

        {/* TAB 4: LIVE INSPECTOR */}
        {activeTab === 'inspector' && (
          <div className="flex flex-col md:flex-row gap-6">
            
            {/* Control Column */}
            <div className="w-full md:w-72 flex-shrink-0 space-y-6">
              <div>
                <label className="block text-xs text-gray-500 uppercase font-bold mb-1">Inspect Vehicle</label>
                <select 
                  className="w-full bg-gray-900 border border-gray-700 text-gray-200 font-mono text-sm p-2 focus:outline-none focus:border-blue-500"
                  value={inspectorVid}
                  onChange={e => { setInspectorVid(e.target.value); setInspectorSource('ALL (Latest)'); }}
                >
                  {vehicles.map(v => <option key={v.vehicle_id} value={v.vehicle_id}>{v.vehicle_id}</option>)}
                </select>
              </div>

              {selectedInspectorV && (
                <div className="border border-gray-800 bg-gray-900 p-4">
                  <h4 className="text-xs uppercase text-gray-500 font-bold mb-3 border-b border-gray-700 pb-1">🩺 Status: {inspectorVid}</h4>
                  <div className="flex justify-between items-center mb-2">
                    <span className="text-sm text-gray-400">Accepted</span>
                    <span className="font-mono text-green-400">{selectedInspectorV.rows_processed}</span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-sm text-gray-400">Rejected</span>
                    <span className="font-mono text-red-400">{selectedInspectorV.rejected_rows}</span>
                  </div>
                </div>
              )}

              <div>
                <label className="block text-xs text-gray-500 uppercase font-bold mb-2">Payload Source</label>
                <div className="space-y-1">
                  {availableInspectorSources.map(src => (
                    <label key={src} className="flex items-center space-x-2 p-2 hover:bg-gray-800 cursor-pointer border border-transparent hover:border-gray-700 transition-all">
                      <input 
                        type="radio" 
                        name="inspectorSource"
                        value={src}
                        checked={inspectorSource === src}
                        onChange={(e) => setInspectorSource(e.target.value)}
                        className="text-blue-500 bg-gray-900 border-gray-700 focus:ring-blue-500 focus:ring-1"
                      />
                      <span className="text-sm font-mono uppercase text-gray-300">{src}</span>
                    </label>
                  ))}
                </div>
              </div>
            </div>

            {/* JSON Viewer Column */}
            <div className="flex-grow">
              <div className="border border-gray-800 bg-[#080808] h-full flex flex-col">
                <div className="bg-gray-900 border-b border-gray-800 p-2 px-4 text-xs font-mono uppercase text-gray-400 flex justify-between items-center">
                  <span>
                    {inspectorSource === 'ALL (Latest)' 
                      ? `Incoming Stream (Source: ${selectedInspectorV?.latest_payload?.metadata?.module || 'Unknown'})`
                      : `Latest Packet: ${inspectorSource}`
                    }
                  </span>
                  <span className="text-green-500 animate-pulse">● LIVE</span>
                </div>
                <div className="p-4 overflow-auto max-h-[600px]">
                  <pre className="text-[13px] font-mono text-[#00ffcc] leading-relaxed">
                    {(() => {
                      if (!selectedInspectorV) return "// NO DATA";
                      const payload = inspectorSource === 'ALL (Latest)' 
                        ? selectedInspectorV.latest_payload 
                        : selectedInspectorV.module_payloads[inspectorSource];
                      
                      return payload ? JSON.stringify(payload, null, 2) : "// WAITING FOR PACKET...";
                    })()}
                  </pre>
                </div>
              </div>
            </div>

          </div>
        )}

      </div>
    </div>
  );
}

// --- Small Reusable Components ---

function KpiCard({ title, value, color = "text-gray-200" }: { title: string, value: string | number, color?: string }) {
  return (
    <div className="bg-[#121212] border border-gray-800 p-4 relative overflow-hidden group hover:border-gray-600 transition-colors">
      <div className="absolute top-0 left-0 w-1 h-full bg-gray-700 group-hover:bg-blue-500 transition-colors"></div>
      <div className="text-xs uppercase text-gray-500 font-bold tracking-wider mb-2 pl-2">{title}</div>
      <div className={`text-2xl font-mono ${color} pl-2`}>{value}</div>
    </div>
  );
}

function TabButton({ id, label, active, onClick }: { id: any, label: string, active: boolean, onClick: (id: any) => void }) {
  return (
    <button
      onClick={() => onClick(id)}
      className={`px-4 py-2 text-sm font-mono uppercase border-t-2 transition-all ${
        active 
          ? 'bg-[#121212] border-blue-500 text-blue-400 border-l border-r border-gray-800' 
          : 'bg-gray-900 border-transparent text-gray-500 hover:text-gray-300 hover:bg-gray-800'
      }`}
    >
      {label}
    </button>
  );
}